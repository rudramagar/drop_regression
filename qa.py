import re
import sys
from . import utils
from . import api
from .color_manager import ColorManager
from . import drop

class ScenarioTester(object):
    """ Class for running QA Tests """

    def __init__(self, msh):
        """ Constructor of QA class """
        self.msh = msh
        self.cfg = msh.cfg
        self.colors = ColorManager(enabled=ColorManager.auto_enabled())
        self.ok_string = self.colors.colorize("ok".ljust(4), fg="bright_green")
        self.fail_string = self.colors.colorize("fail", fg="bright_red")
        self.users = msh.users

    @staticmethod
    def _convert_value(value_type, value, length, user):
        """ Convert values to a type comparable with QA tests """
        if value:
            if (
                value_type != 'alpha' and
                value != 'IGN' and
                not re.search("tk", value) and
                not re.search("or", value) and
                not re.search("mt", value)
            ):
                if value_type == 'alpha':
                    value = value.ljust(length)
                elif value_type == 'calendar':
                    out_format = '%Y%m%d'
                    value= int(user.soup.resolve_dynamic_date(value,out_format))
                elif value_type == 'timestamp_ns':
                    value= int(user.soup.convert_timestamp_type(value))
                elif value_type == 'float':
                    value= float(value)
                else:
                    value = int(value)
            elif value_type == 'alpha':
                value = value.ljust(length)
        elif value_type == 'alpha':
            value = value.ljust(length)
        return value

    def check_scenario(self,received_message, expected_message, user,add=0,message_type_index=2,with_soup=False):
        """ Run scenario checks """
        mode = self.cfg.user_config[self.cfg.board][user]['mode']

        # DROP/SBE messages are decoded from XML, not protocol_config.
        # A DROP match may be None (stream exhausted); handle before sanitize.
        if mode == 'D':
            return self._check_drop_scenario(received_message, expected_message, with_soup)

        received_message = utils.sanitize_message(received_message)

        message_type = received_message[message_type_index]
        fields = self.cfg.protocol_config[mode]['In'][str(message_type)]
        expected_message = api.correct_expected_API_calendar_day_type(mode,message_type,self.users[user].soup,expected_message,self.cfg.main_config["jp_holidays"])
        sys.stdout.write("TEST ")
        print(tuple(expected_message))
        index:int=0
        for index, (field, field_details) in enumerate(fields.items()):
            value_type = field_details['type']
            length = field_details['length']
            expected_value = self._convert_value(
                    value_type,
                    expected_message[index + add],
                    length,self.users[user])
            if re.search("tk", str(expected_value)):
                expected_value = int(self.msh.memory[expected_value])
            if expected_value == 'IGN':
                sys.stdout.write(f"	{self.ok_string}")
            elif re.search(r"^or\d+$", str(expected_value)):
                sys.stdout.write(f"	{self.ok_string}")
            elif re.search(r"^mt\d+$", str(expected_value)):
                sys.stdout.write(f"	{self.ok_string}")
            elif received_message[index+add] == expected_value:
                sys.stdout.write(f"	{self.ok_string}")
            else:
                sys.stdout.write(f"	{self.fail_string}")
                self.msh.qa_pass = False
            print(f" {field} ({received_message[index + add]},{expected_value})")
        #List replies are only for API requests.
        if mode == 'a' and message_type == 5:
            expected_msg_list_type:str = str(expected_message[7])
            received_msg_list_type:str = str(received_message[7])
            expected_msg_count:int = int(expected_message[6])
            received_msg_count:int = int(received_message[6])
            index = index + add +1
            if(expected_msg_list_type == received_msg_list_type and expected_msg_count == received_msg_count):
                size_list_msg:int = len(self.cfg.protocol_config[mode]['In'][str(expected_msg_list_type)].items())
                for i in range(expected_msg_count):
                    next_index = index+size_list_msg
                    self.check_scenario(received_message[index:next_index],expected_message[index:next_index],user,message_type_index=0)
                    index = next_index

    def _check_drop_scenario(self, received_message, expected_message, with_soup=False):
        """ Render DROP/SBE checks for a matched message, or None if not found """

        values = drop.expected_values(list(expected_message))
        sys.stdout.write("TST ")
        print(tuple(values))

        for field_name, received_value, expected_value, ok in drop.compare(
            self.cfg, received_message, values, with_soup
        ):
            if ok:
                sys.stdout.write(f"\t{self.ok_string}")
            else:
                sys.stdout.write(f"\t{self.fail_string}")
                self.msh.qa_pass = False

            if received_value is None:
                print(f" {field_name} (NULL,{expected_value})")
            else:
                print(f" {field_name} ({received_value},{expected_value})")
