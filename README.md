# FileDigestTool

批量计算指定目录下的所有文件的摘要信息

## 快速使用

```shell
python3 file_distinct.py
```

## 后台执行

```shell
nohup python3 file_distinct.py > run.log &
tail -f run.log
```

## 查看帮助信息

```shell
python3 file_distinct.py --help
```

```text
usage: file_distinct.py [-h] [--digest {MD5}] [--scan SCAN] [--output OUTPUT] [--format {CSV}]

File digest Calculator

optional arguments:
  -h, --help       show this help message and exit
  --digest {MD5}   the digest method to use,default is MD5,only support MD5 now
  --scan SCAN      the dir path to scan,default is current dir
  --output OUTPUT  csv file path for output result,default is in current dir,named files_md5.csv
  --format {CSV}   the output result format,default is CSV,only support CSV now
```

## CSV输出格式

```text
abspath,file_dir,file_name,modify_time,create_time,size(B),digest
文件绝对路径,文件所在的目录名,文件名,修改时间,创建时间,文件大小(字节),摘要结果
```