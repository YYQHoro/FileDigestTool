import hashlib
import argparse
import logging
import os
import time


def get_sys_logger():
    logger = logging.getLogger('run')
    handler = logging.FileHandler(filename='FileDistinct.run.log', mode='w', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(stdout_handler)
    logger.setLevel(level=logging.INFO)
    return logger


log = get_sys_logger()

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_file_md5(file_path):
    ins = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(4 * 1024)
            if data:
                ins.update(data)
            else:
                return ins.hexdigest()


def get_result_writer(result_file_name):
    writer = logging.getLogger('resultLog')
    writer.setLevel(logging.INFO)
    handler = logging.FileHandler(filename=result_file_name, encoding='utf-8', mode='w')
    handler.setFormatter(logging.Formatter('%(message)s'))
    writer.addHandler(handler)
    return writer


def parse_args():
    parser = argparse.ArgumentParser(description='File digest Calculator')
    parser.add_argument('--digest', type=str, default='MD5',
                        help='the digest method to use,default is MD5,only support MD5 now', choices=['MD5'])
    parser.add_argument('--scan', type=str, default='.',
                        help='the dir path to scan,default is current dir: ' + os.path.realpath('.'))
    parser.add_argument('--output', type=str, default='files_md5.csv',
                        help='csv file path for output result,default is in current dir,named files_md5.csv')
    parser.add_argument('--format', type=str, default='CSV', choices=['CSV'],
                        help='the output result format,default is CSV,only support CSV now')
    args = parser.parse_args()
    return os.path.realpath(args.scan), os.path.realpath(args.output)


def calculate_dir(scan_dir, result_file_name):
    log.info('start scan files in %s, digest result is writing to %s', scan_dir, result_file_name)
    result_writer = get_result_writer(result_file_name)
    result_writer.info(
        'abspath,file_dir,file_name,modify_time,create_time,size(B),digest')
    file_count = 0
    for root, dirs, files in os.walk(scan_dir):
        dir_file_count = 0
        for file_name in files:
            file = os.path.join(root, file_name)
            modify_time = os.path.getmtime(file)
            create_time = os.path.getctime(file)
            size = os.path.getsize(file)
            result = get_file_md5(file)
            result_writer.info('%s,%s,%s,%s,%s,%s,%s', file, root, file_name,
                               time.strftime(TIME_FORMAT, time.localtime(modify_time)),
                               time.strftime(TIME_FORMAT, time.localtime(create_time)), size, result)
            file_count += 1
            dir_file_count += 1
        log.info('[%s] files count %s', root, dir_file_count)
    log.info('done. total files %s. csv result file was written to %s', file_count, result_file_name)


if __name__ == '__main__':
    calculate_dir(*parse_args())
