"""thin-edge.io Can operations handlers"""

import sys

from . import c8y_can_configuration
from . import c8y_can_device
from .context import Context


def main():
    """main"""
    command = sys.argv[1]
    if command == "c8y_CanConfiguration":
        run = c8y_can_configuration.run
    elif command == "c8y_CanDevice":
        run = c8y_can_device.run

    arguments = sys.argv[2:]
    context = Context()
    run(arguments, context)


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print(f"Error: {ex}", file=sys.stderr)
        sys.exit(1)