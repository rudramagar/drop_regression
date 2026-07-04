"""DROP/SBE helpers: read field names from drop.xml and match scenario lines
against a live or replay message stream.

Two matching modes share one set of decode/compare primitives:
  - Existence (TST): order-independent; a line matches any message in the
    stream. Used for reference/snapshot data (users, firms, securities).
  - Sequence (SEQ): ordered lifecycle; steps must match in stream order, with
    '@name' tokens binding an identity (e.g. orderId) across steps. Used for
    order lifecycles: accept -> trade -> trade confirmed for the same order.
"""

import xml.etree.ElementTree as ET


# Decoded tuple layout for a DROP payload:
#   index 0-1 : Soup header (packetLength, msgType)
#   index 2-5 : SBE header (blockLength, templateId, schemaId, version)
#   index 6+  : SBE message body
SOUP_HEADER = {0: "packetLength", 1: "msgType"}
SBE_HEADER = {2: "blockLength", 3: "templateId", 4: "schemaId", 5: "version"}
BODY_START_INDEX = 6
TEMPLATE_ID_INDEX = 3


def get_metadata(cfg):
    """Return the shared DropMetadata instance, building it once per run."""

    if not hasattr(cfg, 'drop_metadata'):
        cfg.drop_metadata = DropMetadata(cfg.main_config['modes']['D']['config'])

    return cfg.drop_metadata


def decode_value(value):
    """Return a decoded string for one decoded tuple value.

    SBE strings are fixed-width and padded with null bytes (and sometimes
    spaces), so both are stripped to compare against scenario values.
    """

    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')

    return str(value).strip('\x00').strip()


def is_payload(message):
    """Return True if the message is a DROP/SBE payload (not empty or heartbeat)."""

    if not message or len(message) <= len(SBE_HEADER) + 1:
        return False

    return decode_value(message[1]) != 'H'


def expected_values(expected_msg):
    """Return stripped expected values, dropping an operator/user prefix if present."""

    if expected_msg and expected_msg[0] not in ('TST', 'RCV', 'SEQ'):
        fields = expected_msg
    elif len(expected_msg) > 2:
        fields = expected_msg[2:]
    else:
        fields = expected_msg

    return [value if is_binding(value) else decode_value(value) for value in fields]

def resolve_dates(values, resolver):
    """Resolve {TODAY}/{NOW}/{WEEKEND}/{HOLIDAY} tokens in DROP scenario values.

    DROP dates are LocalDate (yyyyMMdd), so tokens resolve to that format.
    The resolver is passed in (soup.resolve_dynamic_date) so this stays free of
    any soup/session dependency; non-token values pass through unchanged.
    """

    resolved = []
    for value in values:
        if isinstance(value, str) and value.startswith('{'):
            resolved.append(resolver(value, '%Y%m%d'))
        else:
            resolved.append(value)
    return resolved

def has_soup_fields(values):
    """Return True if a TST line's values include the soup header fields.

    A soup line begins packetLength|msgType|blockLength|... where msgType (the
    second value) is a letter such as 'S' or 'H'. A plain line begins
    blockLength|templateId|... where the second value is templateId, a number.
    The second value being alphabetic is the reliable, collision-free signal.
    """

    if len(values) < 2:
        return False

    second = values[1]

    if second == 'IGN' or is_binding(second):
        return False

    return not second.lstrip('-').isdigit()


def compare(cfg, message, values, with_soup=False):
    """Compare one message against expected values.

    Yields (field_name, received_value, expected_value, ok) per expected value.
    'IGN' always passes, even when the message is None. A non-IGN field fails
    when the message is None (not found) or the received value differs. When
    with_soup is True the soup header fields (packetLength, msgType) are matched.
    """

    metadata = get_metadata(cfg)
    indexes = metadata.compare_indexes(values, with_soup)

    for position, expected_value in enumerate(values):
        index = indexes[position]
        field_name = metadata.field_name(values, index, with_soup)

        if expected_value == 'IGN':
            received_value = None if message is None or len(message) <= index \
                else decode_value(message[index])
            yield field_name, received_value, expected_value, True
            continue

        if message is None or len(message) <= index:
            yield field_name, None, expected_value, False
            continue

        received_value = decode_value(message[index])
        yield field_name, received_value, expected_value, received_value == expected_value


def matches(cfg, message, values, with_soup=False):
    """Return True if a message matches every expected value of a TST/TSTS line."""

    if not is_payload(message):
        return False

    template_position = 3 if with_soup else 1

    if decode_value(message[TEMPLATE_ID_INDEX]) != values[template_position]:
        return False

    return all(ok for _, _, _, ok in compare(cfg, message, values, with_soup))


def drain_stream(receive, max_reads):
    """Read the replay/live stream once into a list of DROP payloads.

    receive() blocks and returns None for filtered messages, so None reads are
    skipped rather than treated as the end. A SoupConnectionError means the
    replay finished and the peer closed, which cleanly ends the drain.
    """

    stream = []

    try:
        for _ in range(max_reads):
            message = receive()

            if is_payload(message):
                stream.append(message)

    except Exception as error:
        # End of replay (peer disconnected) or read error: stop with what we have.
        if type(error).__name__ != 'SoupConnectionError':
            raise

    return stream

def find_match_streaming(cfg, buffer, receive, exhausted_flag, values, with_soup=False):
    """Find a match, reading forward from the socket only as needed (C1).

    For huge feeds: searches the already-buffered messages first (so earlier
    scenarios' reads are reused), and only if no match is found reads more from
    the socket, appending payloads to buffer, until the match arrives or the
    stream ends. This bounds reading to the deepest message any scenario needs,
    instead of draining the whole feed up front.
    """

    match = find_match(cfg, buffer, values, with_soup)
    if match is not None and _is_full_match(cfg, match, values, with_soup):
        return match

    identity = match

    if exhausted_flag[0]:
        return identity

    template_position = 3 if with_soup else 1
    key_position = _key_position(values, with_soup)
    key_value = values[key_position] if key_position is not None else None
    
    try:
        while True:
            try:
                message = receive()
            except Exception as read_error:
                if type(read_error).__name__ == 'SoupConnectionError':
                    raise
                continue

            if message is None:
                continue

            if not is_payload(message):
                continue

            buffer.append(message)

            if decode_value(message[TEMPLATE_ID_INDEX]) != values[template_position]:
                continue

            if all(ok for _, _, _, ok in compare(cfg, message, values, with_soup)):
                return message

            if identity is None and key_value is not None \
                    and _field_equals(cfg, message, values, key_position, key_value, with_soup):
                identity = message

    except Exception as error:
        if type(error).__name__ != 'SoupConnectionError':
            raise
        exhausted_flag[0] = True

    return identity

def _is_full_match(cfg, message, values, with_soup=False):
    """Return True if a message matches every field (not just identity fallback)."""

    template_position = 3 if with_soup else 1

    if message is None:
        return False

    if decode_value(message[TEMPLATE_ID_INDEX]) != values[template_position]:
        return False

    return all(ok for _, _, _, ok in compare(cfg, message, values, with_soup))

def find_match(cfg, stream, values, with_soup=False):
    """Return the best buffered message for a TST/TSTS line, or None.

    Order-independent. Prefers a message matching every field. If none matches
    fully, falls back to the message with the same template and the same key
    field (the first concrete, non-IGN body field, e.g. userId), so its actual
    values are shown and the wrong field fails visibly instead of all-NULL.
    Returns None only when no message even shares that identity.
    """

    full = None
    identity = None

    template_position = 3 if with_soup else 1
    key_position = _key_position(values, with_soup)
    key_value = values[key_position] if key_position is not None else None

    for message in stream:
        if not is_payload(message):
            continue

        if decode_value(message[TEMPLATE_ID_INDEX]) != values[template_position]:
            continue

        if all(ok for _, _, _, ok in compare(cfg, message, values, with_soup)):
            full = message
            break

        if identity is None and key_value is not None \
                and _field_equals(cfg, message, values, key_position, key_value, with_soup):
            identity = message

    return full if full is not None else identity


def _key_position(values, with_soup=False):
    """Return the index of the first concrete (non-IGN) body field, or None.

    Header fields are skipped; the first non-IGN body field is used as the
    record identity for fallback matching. The header spans 6 fields with soup,
    4 without.
    """

    header_len = len(SOUP_HEADER) + len(SBE_HEADER) if with_soup else len(SBE_HEADER)

    for position in range(header_len, len(values)):
        if values[position] != 'IGN':
            return position

    return None


def _field_equals(cfg, message, values, position, expected, with_soup=False):
    """Return True if a message's field at a TST position equals expected."""

    index = get_metadata(cfg).compare_indexes(values, with_soup)[position]

    if len(message) <= index:
        return False

    return decode_value(message[index]) == expected


# --- Ordered lifecycle matching (SEQ) with @name correlation -----------------
#
# A SEQ scenario is a list of steps that must appear in the stream in order.
# A step field written as '@name' binds that field's received value the first
# time it is seen; later steps writing the same '@name' must match the bound
# value. This threads an identity (e.g. orderId) through an order lifecycle:
# accept -> trade -> trade confirmed, all for the same order.


def is_binding(token):
    """Return True if an expected value is a correlation token like '@orderId'."""

    return isinstance(token, str) and token.startswith('@') and len(token) > 1


def resolve_values(values, bindings):
    """Return expected values with bound '@name' tokens replaced by their value.

    An unbound '@name' becomes 'IGN' (it captures on match rather than compares);
    a bound '@name' becomes the previously captured value so it must match.
    """

    resolved = []

    for value in values:
        if is_binding(value):
            resolved.append(bindings.get(value[1:], 'IGN'))
        else:
            resolved.append(value)

    return resolved


def capture_bindings(cfg, message, values, bindings):
    """Bind every '@name' token in a step to the message's received value.

    Only unbound names are captured; already-bound names are left unchanged so
    the first occurrence defines the identity for the rest of the sequence.
    """

    indexes = get_metadata(cfg).compare_indexes(values)

    for position, value in enumerate(values):
        if not is_binding(value):
            continue

        name = value[1:]
        index = indexes[position]

        if name not in bindings and message is not None and len(message) > index:
            bindings[name] = decode_value(message[index])


def step_matches(cfg, message, values, bindings):
    """Return True if a message matches a step, honoring current bindings."""

    if not is_payload(message):
        return False

    if decode_value(message[TEMPLATE_ID_INDEX]) != values[1]:
        return False

    resolved = resolve_values(values, bindings)
    return all(ok for _, _, _, ok in compare(cfg, message, resolved))


def match_sequence(cfg, stream, steps):
    """Match ordered lifecycle steps against the stream, returning per-step results.

    steps is a list of expected-value lists. Each step must match a message that
    appears after the previous step's match (ordering). '@name' tokens bind on
    first match and must match the bound value thereafter (correlation).

    Returns a list of (message_or_None, resolved_values) aligned to steps, so the
    caller can render each with compare(). A step that cannot be found after the
    prior match yields (None, resolved_values) and fails on NULL; later steps
    then also fail, because the ordering chain is broken.
    """

    results = []
    bindings = {}
    cursor = 0

    for values in steps:
        resolved = resolve_values(values, bindings)
        found = None

        position = cursor
        while position < len(stream):
            message = stream[position]

            if step_matches(cfg, message, values, bindings):
                found = message
                cursor = position + 1
                break

            position += 1

        if found is not None:
            capture_bindings(cfg, found, values, bindings)
            # Re-resolve so the rendered expectation shows the now-bound values.
            resolved = resolve_values(values, bindings)

        results.append((found, resolved))

    return results


class DropMetadata:
    """Read DROP/SBE message and composite field names from drop.xml."""

    def __init__(self, xml_file):
        """Load messages and composites from the schema file."""

        self.messages = {}
        self.composites = {}
        self.field_map_cache = {}
        self._load(xml_file)

    def compare_indexes(self, values, with_soup=False):
        """Return decoded tuple indexes for a TST/TSTS line's expected values.

        Header fields map first; a partial body maps to the last body fields so
        scenarios can skip unstable leading fields. When with_soup is True the
        two soup header fields (packetLength, msgType) precede the SBE header.
        """

        header = list(SOUP_HEADER) + list(SBE_HEADER) if with_soup else list(SBE_HEADER)
        template_position = 3 if with_soup else 1

        if len(values) <= len(header):
            return header[:len(values)]

        body = self._body_indexes(int(values[template_position]))
        body_count = len(values) - len(header)

        if body_count > len(body):
            raise ValueError(
                "DROP scenario has too many fields for template %s"
                % values[template_position]
            )

        return header + body[-body_count:]

    def field_name(self, values, index, with_soup=False):
        """Return the readable field name for a decoded tuple index."""

        if index in SOUP_HEADER:
            return SOUP_HEADER[index]

        if index in SBE_HEADER:
            return SBE_HEADER[index]

        template_position = 3 if with_soup else 1

        if len(values) <= template_position:
            return "DROP[%s]" % index

        return self._field_map(int(values[template_position])).get(index, "DROP[%s]" % index)

    def _load(self, xml_file):
        """Parse messages, composites, and types from the schema.

        Resolves any <xi:include> so composites/types defined in an included
        file (e.g. mercury.common.v1.xml) are available for field expansion.
        Composite members may be <ref>, <field>, or <type> (inline scalars).
        """

        import os

        root = ET.parse(xml_file).getroot()
        base_dir = os.path.dirname(xml_file)

        self._parse_root(root)

        for element in root.iter():
            if self._tag(element.tag) == 'include':
                href = element.attrib.get('href')
                if href:
                    included = os.path.join(base_dir, href)
                    if os.path.exists(included):
                        self._parse_root(ET.parse(included).getroot())

    def _parse_root(self, root):
        """Parse composites and messages from one parsed XML tree."""

        for element in root.iter():
            tag = self._tag(element.tag)

            if tag == 'composite':
                self.composites[element.attrib.get('name')] = [
                    (child.attrib.get('name'),
                     child.attrib.get('type', child.attrib.get('primitiveType')))
                    for child in element if self._tag(child.tag) in ('ref', 'field', 'type')
                ]
            elif tag == 'message':
                self.messages[int(element.attrib['id'])] = element

    def _body_indexes(self, template_id):
        """Return sorted body-field indexes for a template."""

        return sorted(i for i in self._field_map(template_id) if i >= BODY_START_INDEX)

    def _field_map(self, template_id):
        """Return and cache the index-to-field-name map for a template."""

        if template_id in self.field_map_cache:
            return self.field_map_cache[template_id]

        field_map = dict(SBE_HEADER)
        message = self.messages.get(template_id)
        index = BODY_START_INDEX

        if message is not None:
            for child in message:
                if self._tag(child.tag) not in ('field', 'data'):
                    continue

                name = child.attrib.get('name')
                field_type = child.attrib.get('type', child.attrib.get('dimensionType'))

                for expanded in self._expand(name, field_type):
                    field_map[index] = expanded
                    index += 1

        self.field_map_cache[template_id] = field_map
        return field_map

    def _expand(self, name, field_type):
        """Expand a composite type into its flattened field names."""

        if field_type not in self.composites:
            return [name]

        names = []
        for child_name, child_type in self.composites[field_type]:
            names.extend(self._expand(child_name, child_type))

        return names

    @staticmethod
    def _tag(tag):
        """Return an XML tag name without its namespace."""

        return tag.split('}', 1)[1] if '}' in tag else tag
