import sys

from insider_trading.cli import main


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.extend(["--days", "60", "--config", "config.toml"])
    main()
