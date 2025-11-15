from logistream_stream.app import app


def main() -> None:
    """
    Entry point for running the Faust worker.

    Typical usage:

        python -m services.stream_processor.main

    which delegates to app.main().
    """
    app.main()


if __name__ == "__main__":
    main()
