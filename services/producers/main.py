import argparse
import threading

from services.producers.logistream_producers.config import DEFAULT_EPS
from services.producers.logistream_producers.shipment_producer import run_shipment_stream
from services.producers.logistream_producers.gps_producer import run_gps_stream



def _run_shipments(eps: int) -> None:
    run_shipment_stream(eps=eps)


def _run_gps(eps: int) -> None:
    run_gps_stream(eps=eps)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LogiStream AIOps - synthetic producers for Kafka ingest topics."
    )
    parser.add_argument(
        "--mode",
        choices=["shipments", "gps", "both"],
        default="shipments",
        help="Which producer(s) to run.",
    )
    parser.add_argument(
        "--eps-shipments",
        type=int,
        default=DEFAULT_EPS,
        help="Events per second for shipment events.",
    )
    parser.add_argument(
        "--eps-gps",
        type=int,
        default=DEFAULT_EPS,
        help="Events per second for GPS points.",
    )

    args = parser.parse_args()

    if args.mode == "shipments":
        _run_shipments(eps=args.eps_shipments)
    elif args.mode == "gps":
        _run_gps(eps=args.eps_gps)
    else:
        # both
        t1 = threading.Thread(
            target=_run_shipments, kwargs={"eps": args.eps_shipments}, daemon=True
        )
        t2 = threading.Thread(
            target=_run_gps, kwargs={"eps": args.eps_gps}, daemon=True
        )
        t1.start()
        t2.start()
        print(
            f"[main] Running BOTH producers: shipments EPS={args.eps_shipments}, "
            f"gps EPS={args.eps_gps}. Ctrl+C to stop."
        )
        try:
            # Keep main thread alive
            t1.join()
            t2.join()
        except KeyboardInterrupt:
            print("[main] KeyboardInterrupt received. Producers will flush and exit.")


if __name__ == "__main__":
    main()
