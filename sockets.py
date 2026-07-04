import socket
from .soup_exception import SoupConnectionError


def connect_socket(ipaddress, port, quiet=False):
    """ Connect to the socket.

    quiet suppresses the success message (used for DROP and for admin
    reconnects, where the repeated 'Successfully connected' line is noise).
    Errors are always printed regardless of quiet.
    """
    sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sockfd.connect((ipaddress, int(port)))
        if not quiet:
            print(f'{ipaddress}:{port} Successfully connected')
        return sockfd
    except Exception as error:
        error_message = f'{ipaddress}:{port} {error}'
        print(error_message)
        raise SoupConnectionError(error_message)
