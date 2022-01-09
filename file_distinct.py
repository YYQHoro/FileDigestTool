import hashlib
import argparse
import logging
import multiprocessing
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor


def get_sys_logger():
    logger = logging.getLogger('run')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    logger.setLevel(level=logging.INFO)
    return logger


log = get_sys_logger()

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_CONCURRENT = multiprocessing.cpu_count()
SUB_SEP = "__"
ENCODING = 'utf-8'


def get_file_md5(file_path):
    ins = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(4 * 1024)
            if len(data) > 0:
                ins.update(data)
            else:
                return ins.hexdigest()


def parse_args():
    parser = argparse.ArgumentParser(description='File digest Calculator')
    parser.add_argument('--digest', type=str, default='MD5',
                        help='the digest method to use,default is MD5,only support MD5 now', choices=['MD5'])
    parser.add_argument('--scan', type=str, default='.',
                        help='the dir path to scan,default is current dir: ' + os.path.realpath('../..'))
    parser.add_argument('--output', type=str, default='files_md5.csv',
                        help='csv file path for output result,default is in current dir,named files_md5.csv')
    parser.add_argument('--format', type=str, default='CSV', choices=['CSV'],
                        help='the output result format,default is CSV,only support CSV now')
    parser.add_argument('--concurrent', type=int, default=DEFAULT_CONCURRENT,
                        help=f'the digest worker number,default is cpu count {str(DEFAULT_CONCURRENT)}')
    args = parser.parse_args()
    return os.path.realpath(args.scan), os.path.realpath(args.output), args.concurrent


worker_fp = {}


def calculate_worker(abspath: str, result_file_name: str):
    modify_time = os.path.getmtime(abspath)
    create_time = os.path.getctime(abspath)
    size = os.path.getsize(abspath)
    result = get_file_md5(abspath)
    name = threading.current_thread().name
    if name in worker_fp:
        fp = worker_fp[name]
    else:
        fp = open(result_file_name + SUB_SEP + name, mode='a', encoding=ENCODING)
        worker_fp[name] = fp
    fp.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (abspath, os.path.dirname(abspath), os.path.basename(abspath),
                                               time.strftime(TIME_FORMAT, time.localtime(modify_time)),
                                               time.strftime(TIME_FORMAT, time.localtime(create_time)), size, result))


def calculate_dir(scan_dir: str, result_file_path: str, concurrent: int):
    start_time = time.time()
    log.info('start scan files in %s, digest result is writing to %s', scan_dir, result_file_path)
    log.info('current digest worker number is %s', concurrent)
    worker_pool = ThreadPoolExecutor(max_workers=concurrent)
    # worker_pool = multiprocessing.Pool(processes=concurrent)
    file_count = 0
    for root, dirs, files in os.walk(scan_dir):
        dir_file_count = 0
        for file_name in files:
            file = os.path.join(root, file_name)
            worker_pool.submit(calculate_worker, file, result_file_path)
            file_count += 1
            dir_file_count += 1
        log.info('[%s] files count %s', root, dir_file_count)
    log.info('done scan dir files,total file count %s,waiting worker finish.', file_count)
    worker_pool.shutdown()

    close_all_worker_fp()

    log.info('workers done, merging result file...')
    merge_sub_files(result_file_path)
    log.info('all done.result file was written to %s,cost %ss', result_file_path, (time.time() - start_time))


def close_all_worker_fp():
    for fp in worker_fp.values():
        fp.flush()
        fp.close()


def merge_sub_files(result_file_path):
    result_file_dir = os.path.dirname(result_file_path)
    result_file_name = os.path.basename(result_file_path)
    sub_result_files = [os.path.join(result_file_dir, file_name) for file_name in os.listdir(result_file_dir) if
                        file_name.startswith(result_file_name + SUB_SEP)]
    with open(result_file_path, 'w', encoding=ENCODING) as sum_f:
        sum_f.write('abspath\tfile_dir\tfile_name\tmodify_time\tcreate_time\tsize(B)\tdigest\n')
        for sub_file in sub_result_files:
            log.info('merging %s', sub_file)
            with open(sub_file, 'r') as sub_f:
                while True:
                    data = sub_f.read(4 * 1024)
                    if len(data) > 0:
                        sum_f.write(data)
                    else:
                        break
            os.remove(sub_file)
            log.info('merged')


if __name__ == '__main__':
    calculate_dir(*parse_args())
