"""DROP/SBE scenario matching: map schema field names and match TST lines
against the message stream. Order-independent; reads the stream lazily."""

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
    """Return one decoded tuple value as a comparable string."""

    # SBE strings are fixed-width, padded with null bytes and sometimes spaces.

    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')

    return str(value).strip('\x00').strip()


def is_payload(message):
    """Return True if the message is a DROP/SBE payload (not empty or heartbeat)."""

    if not message or len(message) <= len(SBE_HEADER) + 1:
        return False

    return decode_value(message[1]) != 'H'


def is_binding(token):
    """Return True if an expected value is a correlation token like '@orderId'."""

    return isinstance(token, str) and token.startswith('@') and len(token) > 1


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
    """Resolve {TODAY}/{NOW} style date tokens; DROP dates are LocalDate (yyyyMMdd)."""

    resolved = []
    for value in values:
        if isinstance(value, str) and value.startswith('{'):
            resolved.append(resolver(value, '%Y%m%d'))
        else:
            resolved.append(value)
    return resolved


def has_soup_fields(values):
    """Return True if a TST line starts with the soup header fields."""

    # msgType (soup line) is alphabetic; templateId (plain line) is numeric.

    if len(values) < 2:
        return False

    second = values[1]

    if second == 'IGN' or is_binding(second):
        return False

    return not second.lstrip('-').isdigit()


def compare(cfg, message, values, with_soup=False):
    """Yield (field_name, received, expected, ok) per value; IGN always passes."""

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




def find_match_streaming(cfg, buffer, receive, exhausted_flag, values, with_soup=False):
    """Match against the buffer, reading further from the socket only if needed."""

    # Bounds reading to the deepest message any scenario asserts, so a large
    # feed is never drained up front.

    # First try whatever is already buffered (order-independent, reused reads).
    match = find_match(cfg, buffer, values, with_soup)
    if match is not None and _is_full_match(cfg, match, values, with_soup):
        return match

    # Keep the best identity-fallback seen so far, in case no full match exists.
    identity = match

    if exhausted_flag[0]:
        return identity

    template_position = 3 if with_soup else 1
    key_position = _key_position(values, with_soup)
    key_value = values[key_position] if key_position is not None else None

    try:
        while True:
            # A message the decoder cannot unpack is skipped rather than
            # aborting the run; soup framing stays aligned because the body is
            # consumed by its header length before unpacking.
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
        # Stream ended (peer disconnected) or read error: mark exhausted so later
        # scenarios search only the buffer instead of reading a closed socket.
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
    """Return the best buffered message for a TST line, or None."""

    # Falls back to a same-identity record so a wrong field shows its real
    # value instead of every field reporting NULL.

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
    """Return the index of the first non-IGN body field, used as record identity."""

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


class DropMetadata:
    """Read DROP/SBE message and composite field names from drop.xml."""

    def __init__(self, xml_file):
        """Load messages and composites from the schema file."""

        self.messages = {}
        self.composites = {}
        self.field_map_cache = {}
        self._load(xml_file)

    def compare_indexes(self, values, with_soup=False):
        """Return decoded tuple indexes for a TST line's expected values."""

        # A partial body right-aligns to the last fields, so scenarios can omit
        # leading fields.

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
        """Parse the schema, resolving <xi:include> for the common definitions."""

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
