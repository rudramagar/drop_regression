"""
MessageHandler module
"""
import os
import re
import subprocess
import sys
import traceback

from .soup import SoupBinTCP, SoupLogin
from .tokens import TokenHandler
from .printer import MessagePrinter
from .user_manager import UserManager
from . import drop

class MessageHandler:
    """
    Class for handling messages
    """

    def __init__(self, cfg):
        """ Constructor of MessageHandler class """
        self.cfg = cfg
        self.spliter = '|'
        self.memory = {}
        self.tkh = TokenHandler(cfg)
        self.qa_pass = True
        self.qa_total_scenarios = 0
        self.qa_passed_scenarios = 0
        self.qa_failed_scenarios = 0
        self.qa_failed_scenario_list = []
        self.printer = MessagePrinter(cfg)
        self.sub_scenario = ''
        self.sub_scenario_id = ''
        self.sub_scenario_name = ''
        self.users = {}
        self.drop_stream = []
        self.drop_exhausted = [False]

        # Conditional import for running QA tests
        if self.cfg.run_qa:
            from .qa import ScenarioTester               # pylint: disable=import-error
            self.qah = ScenarioTester(self)

    def _update_memory(self, token, value):
        """ Updates the memory that keeps order information """
        self.memory[token] = value

    def _clean_memory(self):
        """ Cleans the memory """
        self.memory = {}

    def _get_token(self, token):
        """ Get token """
        return self.memory[token]

    def _get_base_price(self):
        """Resolve BasePrice for the configured security_id from securities config."""
        security_id = str(getattr(self.cfg, 'security_id', '') or '')
        securities_config = getattr(self.cfg, 'securities_config', {}) or {}
        security = securities_config.get(security_id, {})
        base_price = security.get('BasePrice')
        return str(base_price) if base_price is not None else None

    def _get_tick_size(self):
        """Resolve TickSize for the configured security_id from securities config."""
        security_id = str(getattr(self.cfg, 'security_id', '') or '')
        securities_config = getattr(self.cfg, 'securities_config', {}) or {}
        security = securities_config.get(security_id, {})
        tick_size = security.get('TickSize')
        return str(tick_size) if tick_size is not None else None

    def _get_bad_tick(self):
        """Resolve BadTick for the configured security_id from securities config."""
        security_id = str(getattr(self.cfg, 'security_id', '') or '')
        securities_config = getattr(self.cfg, 'securities_config', {}) or {}
        security = securities_config.get(security_id, {})
        bad_tick = security.get('BadTick')
        return str(bad_tick) if bad_tick is not None else None

    def _get_tick_price(self, direction, count=1):
        """Calculate up/down tick price from BasePrice and TickSize."""
        base_price = self._get_base_price()
        tick_size = self._get_tick_size()
        if base_price is None or tick_size is None:
            return None
        try:
            base_price_i = int(base_price)
            tick_size_i = int(tick_size)
            count_i = int(count)
        except (TypeError, ValueError):
            return None
        if count_i < 1:
            return None
        if direction == 'up':
            return str(base_price_i + (tick_size_i * count_i))
        if direction == 'down':
            return str(base_price_i - (tick_size_i * count_i))
        return None

    def _convert_token(self, msg, continuous=False):
        """ Convert the message text to protocol friendly version by replacing token place holders
        with actual tokens
        """
        operator = msg.split(self.spliter)[0]
        user = msg.split(self.spliter)[1]
        if operator == 'SND':
            tokens = re.findall(r'tk\d*', msg)
            for token in tokens:
                if token in self.memory and not continuous:
                    msg = re.sub(token, self._get_token(token), msg)
                else:
                    self._update_memory(token, self.tkh.get_token(user))
                    msg = re.sub(token, self.tkh.get_token(user), msg)
        # Split payload fields and remove hidden whitespace from scenario lines.
        msg = [field.strip() for field in msg.split(self.spliter)[2:]]

        # Replace a board placeholder in scenario payloads with configured board value.
        # Prefer the lowercase 'brd' token as requested; also accept '{brd}'.
        # Keep backward compatibility with {BOARD} and BOARD.
        for i, field in enumerate(msg):
            if field in ('{brd}', 'brd', '{BOARD}', 'BOARD'):
                msg[i] = self.cfg.board
            elif field in ('SecId', '{SecId}') and getattr(self.cfg, 'security_id', ''):
                msg[i] = str(self.cfg.security_id)
            elif field in ('BPrice', '{BPrice}'):
                base_price = self._get_base_price()
                if base_price is not None:
                    msg[i] = base_price
            elif field in ('BTick', '{BTick}'):
                bad_tick = self._get_bad_tick()
                if bad_tick is not None:
                    msg[i] = bad_tick
            else:
                tick_match = re.match(r'^\{?([UD])Tick(\d+)?\}?$', field, re.IGNORECASE)
                if tick_match:
                    direction = 'up' if tick_match.group(1).upper() == 'U' else 'down'
                    count = int(tick_match.group(2) or '1')
                    tick_price = self._get_tick_price(direction, count)
                    if tick_price is not None:
                        msg[i] = tick_price
        return user, msg

    def _continuous(self, msg2):
        """ Send continuous orders """
        while True:
            user, msg = self._convert_token(msg2, True)
            self.tkh.increment_token(user)
            self.printer.print_details(user, "Out", self.users[user].send(msg))
            self.printer.print_details(user, "In", self.users[user].receive())
        sys.exit(0)

    def _amend(self, msg2):
        """ Send continuous amends """
        user, msg = self._convert_token(msg2, True)
        self.printer.print_details(user, "Out", self.soup.write(user, msg))
        self.printer.print_details(user, "In", self.soup.read(user))
        while True:
            token = int(self.tkh.get_token(user))
            update_msg = ['35', 'U', 'U', token, token+1, 200, 1, 99999, ' ', 0]
            self.printer.print_details(user, "Out", self.soup.write(user, update_msg))
            self.printer.print_details(user, "In", self.soup.read(user))
            self.tkh.increment_token(user)
        sys.exit(0)

    def _login_all(self):
        """ Login to all sessions """
        if self.cfg.scenario_file:
            scenario_files = self.cfg.scenario_file
        else:
            scenario_files = " ".join(self.__get_scenario_files())
        cmd = [f"""cat {scenario_files} \
                | grep -oE '^[A-Z]*\|[a-z]*[0-9]*' \
                | grep -oE '[a-z]*[0-9]*' \
                | sort \
                | uniq"""]
        self.login_users = subprocess.check_output(cmd, shell=True).decode().split('\n')[:-1]
        for user in self.login_users:
            self.login(user)

    def _scenario_send(self, msg):
        """ Send message (DROP is listen-only; admin sessions reconnect per send) """
        user, _ = self._convert_token(msg)
        mode = self.cfg.user_config[self.cfg.board][user]['mode']

        if mode == 'D':
            print("Error: SND is not supported for DROP mode (user %s). "
                  "DROP scenarios are listen-only; use TST to assert on the "
                  "stream instead of sending." % user)
            sys.exit(1)

        # The admin API (mode 'a') closes the socket after one command, so a
        # fresh connection is opened before every send to avoid a broken pipe.
        if mode == 'a':
            self.login(user, quiet=True)

        if self.cfg.continuous:
            self._continuous(msg)
        if self.cfg.amend:
            self._amend(msg)
        user, msg = self._convert_token(msg)
        self.tkh.increment_token(user)
        sys.stdout.write("SND ")
        self.printer.print_details(user, "Out", self.users[user].send(msg))

    def _scenario_receive(self, msg):
        """ Receive and check one scenario message (DROP matches the feed stream) """
        user, msg = self._convert_token(msg)
        mode = self.cfg.user_config[self.cfg.board][user]['mode']

        if self.cfg.run_qa and mode == 'D':
            msg = drop.resolve_dates(msg, self.users[user].soup.resolve_dynamic_date)
            values = drop.expected_values(msg)
            self._drop_receive(user, msg, drop.has_soup_fields(values))
            return

        received_msg = self.users[user].receive()
        self.printer.print_details(user, "In", received_msg)

        if mode == 'I' and received_msg[2].decode('utf-8') == 'T': # If ITCH timestamp read again
            received_msg = self.users[user].receive()
            self.printer.print_details(user, "Out", received_msg)

        if self.cfg.run_qa:
            try:
                while received_msg[1] == 'H': # If OUCH heartbeat read again
                    received_msg = self.users[user].receive()

                self.qah.check_scenario(received_msg, msg, user, 2)

            except IndexError as err:
                if str(err) == 'tuple index out of range':
                    self._handle_qa_index_error(err)
                traceback.print_exc()
                print("Error in subscenario %s" % self.sub_scenario)
                sys.exit(0)

            except Exception:
                traceback.print_exc()
                print("Error in subscenario %s" % self.sub_scenario)
                sys.exit(0)

    def _drop_receive(self, user, msg, with_soup=False):
        """ Match one DROP TST line against the feed, reading further only if needed """
        values = drop.expected_values(msg)
        matched = drop.find_match_streaming(
            self.cfg, self.drop_stream, self.users[user].receive,
            self.drop_exhausted, values, with_soup
        )

        if matched is not None:
            sys.stdout.write("RCV ")
            self.printer.print_details(user, "In", matched)

        self.qah.check_scenario(matched, msg, user, 2, with_soup=with_soup)

    def _scenario_begin(self, msg):
        """ Begin scenario """
        self.sub_scenario = msg.split('|')[1]
        self.sub_scenario_id = self.sub_scenario
        self.sub_scenario_name = ''
        if ' - ' in self.sub_scenario:
            self.sub_scenario_id, self.sub_scenario_name = self.sub_scenario.split(' - ', 1)
        print("\nBEGIN %s" % self.sub_scenario)

    def _scenario_end(self, failure_reason=''):
        """ End scenario """
        if self.cfg.run_qa:
            self.qa_total_scenarios += 1
            if self.qa_pass:
                self.qa_passed_scenarios += 1
            else:
                self.qa_failed_scenarios += 1
                self.qa_failed_scenario_list.append((self.sub_scenario_id, self.sub_scenario_name, failure_reason))
        print("%s %s" %(self.sub_scenario, "PASSED" if self.qa_pass else "FAILED"))
        self.qa_pass = True
        self._clean_memory()

    def _handle_qa_index_error(self, err):
        """Gracefully end current scenario and QA run on tuple index errors."""
        self.qa_pass = False
        failure_reason = f"{type(err).__name__}: {err}"
        print(f"QA exception in scenario {self.sub_scenario}: {failure_reason}")
        self._scenario_end(failure_reason)
        self._print_qa_summary()
        sys.exit(0)

    def _print_qa_summary(self):
        """Print QA summary after scenario execution."""
        if not self.cfg.run_qa:
            return
        print("\n----- QA Summary -----")
        print(f"Total scenarios : {self.qa_total_scenarios}")
        print(f"Passed          : {self.qa_passed_scenarios}")
        print(f"Failed          : {self.qa_failed_scenarios}")
        if self.qa_failed_scenario_list:
            print("Failed Scenarios:")
            for scenario_id, scenario_name, failure_reason in self.qa_failed_scenario_list:
                if scenario_name:
                    print(f"- {scenario_id} | {scenario_name}")
                else:
                    print(f"- {scenario_id}")
                if failure_reason:
                    print(f"  Reason: {failure_reason}")
        print("Result          : %s" % ("PASSED" if self.qa_failed_scenarios == 0 else "FAILED"))

    def __get_scenario_files(self):
        """Return scenario file paths, skipping non-scenario files in a -D dir."""
        if self.cfg.scenario_file:
            return [self.cfg.scenario_file]

        # A scenario directory can hold reference docs or generated output,
        # which would otherwise be parsed as scenarios.
        (scenario_dir, _, scenario_files) = next(os.walk(self.cfg.scenario_dir))
        file_paths = []
        for scenario_file in scenario_files:
            if scenario_file.startswith('.'):
                continue
            if scenario_file.endswith(('.txt', '.md', '.py', '.json', '.bak')):
                continue
            if '_output' in scenario_file or '_debug' in scenario_file:
                continue
            file_paths.append(f"{scenario_dir}/{scenario_file}")
        return file_paths

    def operate(self):
        """ Operate """
        self._login_all()

        filenames = self.__get_scenario_files()
        for scenario_file in sorted(filenames):
            with open(scenario_file) as file_handler:
                scenario = file_handler.read()
            for msg in scenario.split('\n')[:-1]:
                operator = msg.split('|')[0]
                if operator == 'BGN': self._scenario_begin(msg)
                if operator == 'SND': self._scenario_send(msg)
                if operator == 'RCV' or operator == 'TST':
                    self._scenario_receive(msg)
                if operator == 'END': self._scenario_end()

        if self.cfg.run_qa:
            self._print_qa_summary()

        if self.cfg.quit_on_completion:
            self._quit()
        self.listen()

    def listen(self):
        """ Listen on a soup session. Will continuelsy send heartbeats and keep the session live """
        while True: # Continuously listen
            for user in self.login_users:
                msg = self.users[user].receive()
                if msg:
                    if msg[1].decode() != 'H':
                        self.printer.print_details(user, "In", msg, 'S')
                self.read_heartbeat(msg, user)
                self.send_heartbeat(user)

    def _quit(self):
        """ Quit listening immediately after the first heartbeat is received """
        for user in self.login_users:
            self.printer.print_details(user, "Out", self.users[user].send(['1', 'O']), 'S') # Send logout message
        sys.exit(0)

    def send_heartbeat(self, user):
        """ Sends heartbeats """
        heartbeat = self.users[user].send_heartbeat() # Send heartbeats
        if heartbeat and 'SOUP_HEARTBEAT' in os.environ:
            self.printer.print_details(user, "In", heartbeat, 'S')

    def read_heartbeat(self, msg, user):
        """ Reads heartbeats """
        if 'SOUP_HEARTBEAT' in os.environ and msg[1] == 'H':
            self.printer.print_details(user, "In", msg, 'S')

    def login(self, userid, seq_num='0', quiet=False):
        """ login soup session (quiet suppresses login I/O prints on reconnect) """
        user = UserManager(self.cfg, userid)
        self.users[userid] = user
        msg = user.login()
        if not quiet:
            self.printer.print_details(userid, "Out", msg, 'S') # Send login message
        login_reply = user.receive()
        if not quiet:
            self.printer.print_details(userid, "In", login_reply, 'S') # Send login message
        if login_reply[1] == 'J':
            sys.exit(0)
        self.tkh.read_token(userid, user.user_config['usercode'])
