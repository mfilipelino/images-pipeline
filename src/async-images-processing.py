from core import parse_args, run_pipeline


def main() -> None:
    config = parse_args("async")
    run_pipeline(config)


if __name__ == "__main__":
    main()
