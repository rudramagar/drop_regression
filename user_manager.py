import time
from .sockets import *
from .soup import SoupBinTCP, SoupLogin


class UserManager:
    def __init__(self, cfg, userid=None):
        self.cfg = cfg
        self.user_config = self.generate_user(userid)
        self.soup_login = SoupLogin(self.cfg, self.user_config['mode'])
        self.soup = SoupBinTCP(self.cfg, self.user_config['mode'])

    def login(self):
        msg = self.soup_login.write(self.user_config['socket'], self.user_config['login_msg']) # Send login message
        return msg

    def receive(self):
        msg = self.soup.read(self.user_config['socket'])
        return msg

    def send(self, msg):
        return self.soup.write(self.user_config['socket'], msg)

    def send_heartbeat(self):
        if time.time() - self.user_config['heartbeat'] > self.cfg.heartbeat_interval:
            self.user_config['heartbeat'] = time.time()
            return self.send(['1', 'R']) # Send heartbeats

    def generate_login_message(self, user):
        """ Generates the login message for a given user """
        user_dict = self.cfg.user_config[self.cfg.board][user]
        msg = [
            self.cfg.protocol_config['S']['Out']['L']['PacketLength']['value'],
            self.cfg.protocol_config['S']['Out']['L']['PacketType']['value'],
            user_dict['usercode'], user_dict['password'], self.cfg.soup_session, self.cfg.seq_num
        ]
        return msg

    def generate_user(self, user=None):
        user = user or self.cfg.user
        user_config = self.cfg.user_config[self.cfg.board][user]
        user_config['login_msg'] = self.generate_login_message(user)
        user_config['socket'] = connect_socket(
            user_config['ip'], user_config['port'],
            quiet=(user_config['mode'] == 'D')
        )
        user_config['heartbeat'] = time.time()
        return user_config
