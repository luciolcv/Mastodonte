import argparse
from timeline import get_timeline, parallel_crawling

def timeline(name: str, max_iter: int, local: bool, reblogs: bool, resume: bool):
    # Summary of parameters
    print(f"Instance name: {name}")
    print(f"Max iterations: {max_iter}")
    print(f"Local: {local}")
    print(f"Reblogs: {reblogs}")
    print()

    # Get the timeline
    get_timeline(name, max_iter, local, reblogs, resume)


def timelines(list_path: str, num_threads: int, max_iter: int, local: bool, reblogs: bool, resume: bool):
    # Summary of parameters
    print(f"Path to instances: {list_path}")
    print(f"Max iterations: {max_iter}")
    print(f"Local: {local}")
    print(f"Reblogs: {reblogs}")
    print()

    # Get the timelines
    parallel_crawling(list_path, num_threads, max_iter, local, reblogs, resume)


def main():
    parser = argparse.ArgumentParser(description="Run the Mastodonte toolkit.")
    subparsers = parser.add_subparsers(dest='command')

    # Timeline subcommand
    timeline_parser = subparsers.add_parser('timeline', help='Crawl the timeline of a single instance')
    timeline_parser.add_argument('--name', type=str, required=True, help='Instance name')
    timeline_parser.add_argument('--max-iter', type=int, help='Maximum iterations (default 100)', default=100)
    timeline_parser.add_argument('--local', action='store_true', help='Display local timeline (default True)', default=True)
    timeline_parser.add_argument('--reblogs', action='store_false', help='Include reblogs in the timeline (default False)', default=False)
    timeline_parser.add_argument('--resume', action='store_true', help='Resume previous crawlings for this instance (default False)', default=False)

    # Multiple timelines subcommand
    multiple_timelines_parser = subparsers.add_parser('timelines', help='Crawl multiple timelines from a list of instances')
    multiple_timelines_parser.add_argument('--list-path', type=str, required=True, help='File with a list of instances')
    multiple_timelines_parser.add_argument('--num-threads', type=int, help='Number of threads (default 8)', default=8)
    multiple_timelines_parser.add_argument('--max-iter', type=int, help='Maximum iterations (default 100)', default=100)
    multiple_timelines_parser.add_argument('--local', action='store_true', help='Display local timeline (default True)', default=True)
    multiple_timelines_parser.add_argument('--reblogs', action='store_true', help='Include reblogs in the timeline (default False)', default=False)
    multiple_timelines_parser.add_argument('--resume', action='store_true', help='Resume previous crawlings for this instance (default False)', default=False)

    args = parser.parse_args()

    if args.command == 'timeline':
        timeline(args.name, args.max_iter, args.local, args.reblogs, args.resume)
    elif args.command == 'timelines':
        timelines(args.list_path, args.num_threads, args.max_iter, args.local, args.reblogs, args.resume)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
