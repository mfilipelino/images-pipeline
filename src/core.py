import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Iterable, List

from images_pipeline.module1 import ImageConfig, process_image


def _process(file: str) -> str:
    # Demo image processing using the existing module1 utilities
    conf = ImageConfig(width=100, height=100)
    return f"{file}: {process_image(conf)}"


def _serial(files: Iterable[str]) -> List[str]:
    return [_process(f) for f in files]


def _multithread(files: Iterable[str], workers: int) -> List[str]:
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(_process, files))


def _multiprocess(files: Iterable[str], workers: int) -> List[str]:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(_process, files))


async def _async_single(file: str) -> str:
    return _process(file)


async def _asyncio(files: Iterable[str], workers: int) -> List[str]:
    sem = asyncio.Semaphore(workers)

    async def _task(f: str) -> str:
        async with sem:
            return await _async_single(f)

    return await asyncio.gather(*(_task(f) for f in files))


def _ray(files: Iterable[str], workers: int) -> List[str]:
    import ray

    @ray.remote
    def _task(file: str) -> str:
        return _process(file)

    ray.init(ignore_reinit_error=True, num_cpus=workers)
    try:
        result_ids = [_task.remote(f) for f in files]
        results = ray.get(result_ids)
    finally:
        ray.shutdown()
    return results


def process_images(files: Iterable[str], strategy: str, workers: int = 4) -> List[str]:
    if strategy == "serial":
        return _serial(files)
    if strategy == "multithread":
        return _multithread(files, workers)
    if strategy == "multiprocess":
        return _multiprocess(files, workers)
    if strategy == "async":
        return asyncio.run(_asyncio(files, workers))
    if strategy == "ray":
        return _ray(files, workers)
    raise ValueError(f"Unknown strategy: {strategy}")


def cli_entry(strategy: str) -> None:
    parser = argparse.ArgumentParser(description="Demo image processing")
    parser.add_argument("files", nargs="+", help="Files to process")
    parser.add_argument("--workers", type=int, default=4, help="Worker count")
    args = parser.parse_args()
    results = process_images(args.files, strategy=strategy, workers=args.workers)
    for r in results:
        print(r)

