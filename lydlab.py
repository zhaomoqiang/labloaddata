import hashlib
import requests
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time


class LYDataDownloader:
    def __init__(self, host, sk, ak, data_name, data_type, local_dir):
        self.host = host  # 服务器地址
        self.sk = sk
        self.ak = ak
        self.data_name = data_name
        self.data_type = data_type
        self.local_dir = local_dir

    def _make_user_headers(self):
        data_str = self.sk + self.ak
        data = data_str.encode("utf-8")
        hash_obj = hashlib.sha256()
        hash_obj.update(data)
        authorization = hash_obj.hexdigest()
        headers = {
            "Authorization": authorization,
            "Source": self.ak,
        }
        return headers

    def list_files(self):
        """列举目录下的所有文件（处理分页）"""
        response = requests.post(
            f"{self.host}/v1/api/load_file/list_file",
            headers=self._make_user_headers(),
            json={
                "data_name": self.data_name,
                "data_type": self.data_type,
            },
        )
        if response.status_code != 200:
            raise Exception(f"list_files {response.text}")

        res_data = response.json().get("data")
        return res_data

    def _file_load(self, file_path):
        """列举目录下的所有文件（处理分页）"""
        response = requests.post(
            f"{self.host}/v1/api/load_file/file_load_url",
            headers=self._make_user_headers(),
            json={
                "data_name": self.data_name,
                "data_type": self.data_type,
                "file_path": file_path,
            },
        )
        if response.status_code != 200:
            raise Exception(f"获取文件列表失败：{response.text}")

        res_data = response.json().get("data")
        if res_data is None:
            return res_data
        return res_data.get("file_url")

    def _download(self, file_path, local_path, chunk_size=1024, retries=3):
        # 检查是否有部分下载的文件
        temp_path = local_path + ".temp"
        completed_size = 0

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if os.path.exists(local_path):
            # 获取已下载的部分大小
            completed_size = os.path.getsize(local_path)
            print(f"发现下载文件{local_path}，大小: {completed_size/1024/1024:.2f} MB")
            return

        if os.path.exists(temp_path):
            completed_size = os.path.getsize(temp_path)
            print(f"发现部分下载文件，大小: {completed_size/1024/1024:.2f} MB")

        headers = {}
        if completed_size > 0:
            headers["Range"] = f"bytes={completed_size}-"

        url = self._file_load(file_path)
        if url is None:
            print(f"获取下载失败: {file_path}")
            return
        for attempt in range(retries):
            try:
                with requests.get(
                    url, headers=headers, stream=True, timeout=30
                ) as response:
                    response.raise_for_status()

                    # 获取文件总大小
                    total_size = int(response.headers.get("content-length", 0))
                    if completed_size > 0:
                        total_size += completed_size

                    print(f"开始下载，文件大小: {total_size/1024/1024:.2f} MB")

                    # 下载文件
                    with open(
                        temp_path, "ab" if completed_size > 0 else "wb"
                    ) as f, tqdm(
                        desc=os.path.basename(local_path),
                        total=total_size,
                        initial=completed_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                    ) as bar:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                bar.update(len(chunk))

                    # 下载完成后重命名文件
                    os.replace(temp_path, local_path)
                    print(f"下载完成: {local_path}")
                    return True

            except Exception as e:
                wait_time = (attempt + 1) * 2
                print(f"下载失败 ({attempt+1}/{retries}): {str(e)}")
                print(f"将在 {wait_time} 秒后重试...")
                time.sleep(wait_time)

        print("下载失败，已达到最大重试次数")
        return False

    def download_directory(self):
        """下载整个目录（并发下载多文件）"""
        # 1. 列举目录所有文件
        res_data = self.list_files()
        if not res_data:
            print("目录为空，无需下载")
            return
        # 2. 并发下载（根据CPU核心数设置线程数）
        with ThreadPoolExecutor(max_workers=8) as executor:
            for single_data in res_data:
                # 本地路径：保持目录结构
                file_path = single_data.get("file_path")
                local_path = os.path.join(self.local_dir, file_path)
                print(f"准备下载{file_path}到{local_path}")
                # 提交下载任务
                executor.submit(self._download, file_path, local_path)
        print("所有文件下载完成")

    def download_file(self, file_path):
        """下载文件"""
        local_path = os.path.join(self.local_dir, file_path)
        print(f"准备下载{file_path}到{local_path}")
        # 提交下载任务
        self._download(file_path, local_path)
        print(f"文件下载完成:{file_path}")
