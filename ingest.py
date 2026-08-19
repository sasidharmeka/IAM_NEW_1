"""GitHub Actions entrypoint for one bounded ingestion execution."""

from consumer import main as consume
from producer import main as publish


def main() -> None:
    publish()
    consume()


if __name__ == "__main__":
    main()
