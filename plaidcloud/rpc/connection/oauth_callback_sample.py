"""Example of a handler script for custom protocols."""
import sys


def callback(args):
    # Make sure we have only one arg, the URL
    if len(args) != 1:
        return 1

    # Parse the URL:
    full_url = args[0]
    protocol, full_path = args[0].split(":", 1)
    path, full_args = full_path.split("?", 1)
    action = path.strip("/")
    args = full_args.split("&")
    print(f'Full URL: {full_url}')
    print(f'Protocol: {protocol}')
    print(f'Action variables: {action}')
    print(f'Parsed args: {args}')
    return 0


if __name__ == '__main__':
    sys.exit(callback(sys.argv[1:]))
