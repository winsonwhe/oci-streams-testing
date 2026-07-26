# Excel → OCI Streams（Python）

本示例参考 Oracle 官方 [SDK for Python Streaming Quickstart](https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/streaming-quickstart-oci-sdk-for-python.htm)，将生产者和消费者拆分为两个独立程序：

- `producer.py`：读取 Excel；每一行转为一条 UTF-8 JSON 消息；Base64 编码后批量发送。
- `consumer.py`：通过 consumer group cursor 拉取消息；Base64 解码并输出 JSON Lines。

## 本工作簿的消息设计

已核验 `nvlink-switch-metrics-test.xlsx`：

- 工作表：`nvlink-switch-metrics`
- 表头：780 列，无空表头或重复表头
- 非空数据：9 行（工作表的已用范围/格式延伸到第 2701 行，但其余行为全空）
- 每条消息约 21 KB

每一条消息的 value：

```json
{
  "schema_version": 1,
  "source": {
    "file": "nvlink-switch-metrics-test.xlsx",
    "sheet": "nvlink-switch-metrics",
    "excel_row": 2
  },
  "data": {
    "timestamp": 1784258005931,
    "source_id": "0x03ed6918805d448c",
    "port_guid": "0x03ed6918805d448d"
  }
}
```

`data` 实际包含 Excel 中全部 780 列，空单元格保留为 JSON `null`。默认 key 为 `port_guid`，因此同一端口的记录会进入同一分区并保持分区内顺序。可以用 `--key-columns source_id,port_guid` 修改。

生产者按 key 和 value 的 Base64 解码后大小动态组包，默认每个 PUT 请求不超过 900 KiB，低于 OCI Streams 的 1 MiB 单消息和单请求限制。

## 1. 准备环境

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

准备 OCI SDK 配置文件（通常为 `~/.oci/config`），并从 OCI Console 获取：

- Stream OCID
- Stream 的 Messages Endpoint

推荐通过环境变量提供，不要把 OCID 或凭证写入代码：

```bash
export OCI_STREAM_OCID='ocid1.stream.oc1.ap-sydney-1.amaaaaaaprp6l3qajlpdrttwqxdp6qaw7vyraasilnm3l66tfwofhinf3knq'
export OCI_MESSAGE_ENDPOINT='https://cell-1.streaming.ap-sydney-1.oci.oraclecloud.com'
export OCI_CONFIG_PROFILE='DEFAULT'
```

生产环境运行在 OCI Compute 时，可以使用 instance principal，并确保动态组和 IAM policy 已授予 Stream 权限。

## 2. 先做本地校验

此命令读取和序列化全部数据，但不连接 OCI：

```bash
python producer.py \
  './nvlink-switch-metrics-test.xlsx' \
  --sheet nvlink-switch-metrics \
  --dry-run
```

快速检查前 5 行：

```bash
python producer.py \
  './nvlink-switch-metrics-test.xlsx' \
  --max-rows 5 \
  --dry-run
```

## 3. 启动消费者

从流的最早可用位置消费，并将结果追加到文件：

```bash
python consumer.py \
  --group nvlink-metrics-group \
  --instance consumer-1 \
  --output consumed_messages.jsonl \
  --follow
```

默认在成功写出并 flush 一批消息后手动提交 offset。若希望完全采用 Oracle 快速入门中的自动提交行为，可加 `--commit-on-get`。

注意：已有 consumer group 会从它已提交的 offset 继续消费；`--cursor-type` 只在该 group 第一次建立时生效。每个并行消费者必须使用不同的 `--instance`。

## 4. 启动生产者

建议先发送 5 行做端到端测试：

```bash
python producer.py \
  './nvlink-switch-metrics-test.xlsx' \
  --sheet nvlink-switch-metrics \
  --max-rows 5
```

确认消费者输出正常后发送全部非空行：

```bash
python producer.py \
  './nvlink-switch-metrics-test.xlsx' \
  --sheet nvlink-switch-metrics
```

使用 instance principal：

```bash
python producer.py /path/to/nvlink-switch-metrics-test.xlsx \
  --auth instance-principal

python consumer.py \
  --auth instance-principal \
  --group nvlink-metrics-group \
  --follow
```

## 运行语义与注意事项

- `put_messages` 可能部分成功；生产者会逐条检查返回结果，遇到失败立即以非零状态退出并报告 Excel 行号。
- 生产者默认不对 PUT 自动重试，因为“服务端已接收但客户端超时”的重试可能产生重复消息。需要重试时显式加 `--enable-retries`，消费者应按业务键去重。
- 消费者默认手动提交 offset，提供至少一次处理语义。若进程在“写出成功、提交失败”之间中断，重启后可能再次输出同一消息。
- OCI Streams 每个分区的写入上限为 1 MB/s；大量发送时应按 Stream 分区数和 key 分布评估吞吐。
- 消息 retention 到期后会被删除；Stream 的 retention 创建后不能修改。
