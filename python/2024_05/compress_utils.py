import uuid
import gzip
import pathlib
import shutil
import itertools
import pathlib
import gzip, bz2, lzma
import math
import pandas as pd
import time
from concurrent.futures import ProcessPoolExecutor

TMP_PATH = "/dev/shm/"


# 测试大小上限为 16GB
SIZE_LIMIT = 16 * 1024 * 1024 * 1024 

def generate_uuid_as_directory_name():
    uuid_str = str(uuid.uuid4())
    directory_name = uuid_str.replace("-", "_")
    return directory_name

def is_power_of_two(n):
    """
    判断一个数是否是2的幂次。

    Args:
        n (int): 要判断的数。

    Returns:
        bool: 如果是2的幂次，则返回True，否则返回False。
    """

    return n > 0 and (n & (n - 1)) == 0


def get_sample_range(count, fraction):
    sample_range = set()
    for i in range(0, count):
        v = math.ceil(1 * fraction**i)
        sample_range.add(v)
    return sample_range

SAMPLE_RANGE = get_sample_range(500, 1.5)

def should_sample(n):
    return n in SAMPLE_RANGE

def _test_compress(source_file_path, compress_type, compress_level):
    ret_collect = []
    source_file = source_file_path
    file_list = []
    while True:
        pure_file_gz = pathlib.Path(f"{TMP_PATH}{generate_uuid_as_directory_name()}")
        file_list.append(pure_file_gz)
        a = time.process_time()
        with open(source_file, "rb") as f_in:
            if compress_type == "xz":
                with lzma.open(pure_file_gz, "wb", preset=compress_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            elif compress_type == "gz":
                with gzip.open(pure_file_gz, "wb", compresslevel=compress_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            elif compress_type == "bz":
                with bz2.open(pure_file_gz, "wb", compresslevel=compress_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            else:
                raise ValueError("Invalid compress type")
        
        time_consumption = time.process_time() - a
        source_file = pure_file_gz
        if len(file_list) >= 2 and file_list[-1].stat().st_size > file_list[-2].stat().st_size:
            list(map(lambda x: x.unlink(), file_list))
            return ret_collect
        
        ret_collect.append({
            'size': file_list[-1].stat().st_size,
            'round': len(file_list),
            'time': time_consumption
        })



def compress_logic(pure_combine_file, compress_mode="gbx", compress_level=9, rows=0):
    # 反复压缩，直至不能压缩
    data = {
        "rows": rows,
        "pure": pure_combine_file.stat().st_size,
    }
    
    with ProcessPoolExecutor(max_workers=2) as executor:
        if "x" in compress_mode:
            future_x = executor.submit(_test_compress, pure_combine_file,'xz', compress_level)

        if "g" in compress_mode:
            future_g = executor.submit(_test_compress, pure_combine_file,'gz', compress_level)

        if "b" in compress_mode:
            future_b = executor.submit(_test_compress, pure_combine_file,'bz', compress_level)

            
        if "g" in compress_mode:
            future_result = future_g.result()
            data['gz-first-size'] = future_result[0]['size']
            data['gz-first-time'] = future_result[0]['time']
            data['gz-last-size'] = future_result[-1]['size']
            data['gz-last-time'] = future_result[-1]['time']
            data['gz-last-round'] = len(future_result)
        if "b" in compress_mode:
            future_result = future_b.result()
            data['bz-first-size'] = future_result[0]['size']
            data['bz-first-time'] = future_result[0]['time']
            data['bz-last-size'] = future_result[-1]['size']
            data['bz-last-time'] = future_result[-1]['time']
            data['bz-last-round'] = len(future_result)
        if "x" in compress_mode:
            future_result = future_x.result()
            data['xz-first-size'] = future_result[0]['size']
            data['xz-first-time'] = future_result[0]['time']
            data['xz-last-size'] = future_result[-1]['size']
            data['xz-last-time'] = future_result[-1]['time']
            data['xz-last-round'] = len(future_result)
        
    return data
    

def compress_data(data, compress_mode="gbx", compress_level=9,rows=0):
    pure_combine_file = pathlib.Path(f"{TMP_PATH}{generate_uuid_as_directory_name()}")
    
    with open(pure_combine_file, "wb")as f:
        f.write(data)
    report = compress_logic(pure_combine_file,compress_mode, compress_level,rows)
    pure_combine_file.unlink()
    return report
    

def compress_test_data_v3(source_iterate_func, compress_mode="gbx", compress_level=9, file_mode="ab", sample_func=should_sample):
    # source_iterate_func: 数据迭代的函数
    # compress_mode： 压缩的模式
    # compress_level： 压缩的等级
    # file_mode： 文件的打开模式， 测试堆文件，还是测试独立文件
    # sample_func: 采样方法，默认是1.5倍采样，可以每次采样， lambda x: True
    #
    # 不再使用 tar 的方法，使用二进制拼接的方法
    # 拼接三个文件
    # 进行间隔测试，测试不同数据条数时的文件压缩能力，数据间隔为2^n
    # 同时进行迭代压缩，直至不可压缩
    # 压缩文件大小和压缩时间

    pure_combine_file = pathlib.Path(f"{TMP_PATH}{generate_uuid_as_directory_name()}")
    ret_data_list = []
    
    file_size_current = 0
    file_buffer = open(pure_combine_file, file_mode)
    i = 0
    for i, data in enumerate(itertools.chain.from_iterable([v() for v in source_iterate_func])):
        file_buffer.write(data)
        file_size_current += len(data)
        
        if file_size_current > SIZE_LIMIT:
            break
        
        if sample_func(i):
            file_buffer.close()
            data = compress_logic(pure_combine_file, compress_mode, compress_level, i)
            ret_data_list.append(data)
            file_buffer = open(pure_combine_file, file_mode)
            
    if file_buffer:
        file_buffer.close()
        data = compress_logic(pure_combine_file, compress_mode, compress_level, i)
        ret_data_list.append(data)
        
    pure_combine_file.unlink()
    df = pd.DataFrame(ret_data_list)
    
    if "g" in compress_mode:
        df["gz-first-ratio"] = df["pure"] / df["gz-first-size"]
        df["gz-last-ratio"] = df["pure"] / df["gz-last-size"]
    if "b" in compress_mode:
        df["bz-first-ratio"] = df["pure"] / df["bz-first-size"]
        df["bz-last-ratio"] = df["pure"] / df["bz-last-size"]
    if "x" in compress_mode:
        df["xz-first-ratio"] = df["pure"] / df["xz-first-size"]
        df["xz-last-ratio"] = df["pure"] / df["xz-last-size"]
    return df

def set_df_info(df, category, info):
    df['category'] = category
    df['comment'] = ""
    df.at[df.index[-1], 'comment'] = info
    return df