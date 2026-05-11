---
title: "Redis Streams 详解"
date: 2026-05-11
draft: false
---

# Redis Streams 详解

## 概述

Redis Streams 是 Redis 5.0 引入的一种数据结构，专门用于实现消息队列系统。它提供了一个持久化的、支持多播的消息队列，具有消费者组（Consumer Groups）功能，可以实现类似 Kafka 的消息处理模式。

## 基本概念详解

### Key（键）
- **定义**：Redis 中的键是存储数据的唯一标识符
- **在 Streams 中的作用**：作为消息流的唯一标识，所有相关的消息都存储在这个键下
- **示例**：`agreementStreamQueue` 就是一个键，所有与协议相关的消息都存储在这个流中
- **特点**：每个键对应一个独立的消息流，不同键之间的消息是隔离的

### Consumer Group（消费者组）
- **定义**：一组消费者的逻辑分组，用于协调消费同一个消息流
- **作用**：
  - 实现消息的负载均衡（多个消费者可以并行处理消息）
  - 维护消费进度（每个组独立跟踪消费位置）
  - 提供容错能力（一个消费者失败时，其他消费者可以继续处理）
- **示例**：`agreementStreamQueueGroup` 是一个消费者组，可以有多个消费者实例属于这个组
- **关系**：一个流可以有多个消费者组，每个消费者组独立消费消息

### 消费者（Consumer）
- **定义**：消费者组内的具体消费者实例
- **作用**：从消费者组中拉取消息并处理
- **示例**：在代码中通过 `UUID.randomUUID().toString().substring(0, 8)` 生成唯一的消费者名称
- **特点**：同一消费者组内的多个消费者共享消息，每条消息只被一个消费者处理

### 消息 ID
- **格式**：`<millisecondsTime>-<sequenceNumber>`
- **示例**：`1526985054069-0`
- **含义**：
  - `1526985054069`：时间戳（毫秒），表示消息添加的时间
  - `0`：序列号，用于区分同一毫秒内的多条消息

## 基本命令详解

### XADD - 添加消息到流中
```
XADD key [MAXLEN|MINID [=|~] threshold [LIMIT count]] * field value [field value ...]
```

**参数详解：**
- `key`：流的名称（如 `agreementStreamQueue`）
- `MAXLEN`：可选，限制流的最大长度，超过此长度会删除旧消息
- `MINID`：可选，限制流的最小ID，低于此ID的消息会被删除
- `*`：使用自动生成的消息ID
- `field value`：要存储的键值对，可以有多个

**示例：**
```
XADD agreementStreamQueue * terminalCode T001 eventType UPDATE
# 向agreementStreamQueue流中添加一条消息，包含terminalCode和eventType字段
```

**重要注意事项：**
根据实际代码分析，消息的值需要能够被转换为 `TerminalDto` 对象，因此推荐格式：
```
XADD agreementStreamQueue * terminalData "{\"terminalCode\":\"T001\",\"eventType\":\"UPDATE\"}" eventType "TERMINAL_UPDATE"
```

### XRANGE - 从流中读取消息
```
XRANGE key start end [COUNT count]
```

**参数详解：**
- `key`：流的名称
- `start`：开始ID（- 表示最旧的消息）
- `end`：结束ID（+ 表示最新的消息）
- `COUNT`：可选，限制返回的消息数量

**示例：**
```
XRANGE agreementStreamQueue - + COUNT 10
# 获取agreementStreamQueue中最新的10条消息
```

### XREVRANGE - 逆序读取消息
```
XREVRANGE key end start [COUNT count]
```

**参数详解：**
- `key`：流的名称
- `end`：结束ID（+ 表示最新的消息）
- `start`：开始ID（- 表示最旧的消息）
- `COUNT`：可选，限制返回的消息数量

**示例：**
```
XREVRANGE agreementStreamQueue + - COUNT 10
# 获取agreementStreamQueue中最新的10条消息（逆序）
```

### XGROUP - 消费者组相关操作
```
XGROUP CREATE key groupname id-or-$ [MKSTREAM]
XGROUP DESTROY key groupname
XGROUP DELCONSUMER key groupname consumername
```

**参数详解：**
- `CREATE`：创建消费者组
  - `key`：流的名称
  - `groupname`：消费者组名称（如 `agreementStreamQueueGroup`）
  - `id-or-$`：开始消费的消息ID（$ 表示从最新消息开始，0 表示从头开始）
  - `MKSTREAM`：如果流不存在则创建
- `DESTROY`：删除消费者组
- `DELCONSUMER`：删除消费者组中的消费者

**示例：**
```
XGROUP CREATE agreementStreamQueue agreementStreamQueueGroup $ MKSTREAM
# 创建消费者组，从最新消息开始消费，如果流不存在则创建
```

### XREADGROUP - 从消费者组读取消息
```
XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
```

**参数详解：**
- `GROUP group consumer`：指定消费者组和消费者名称
- `COUNT`：可选，限制读取的消息数量
- `BLOCK`：可选，阻塞等待时间（毫秒），0表示无限等待
- `STREAMS key`：指定要读取的流
- `ID`：指定读取消息的起始位置（> 表示只读取新的未消费消息）

**示例：**
```
XREADGROUP GROUP agreementStreamQueueGroup myConsumer COUNT 10 STREAMS agreementStreamQueue >
# 从agreementStreamQueueGroup消费者组中，以myConsumer身份读取10条新消息
```

### XACK - 确认消息处理完成
```
XACK key group ID [ID ...]
```

**参数详解：**
- `key`：流的名称
- `group`：消费者组名称
- `ID`：已处理消息的ID（可多个）

**示例：**
```
XACK agreementStreamQueue agreementStreamQueueGroup 1678886452123-0
# 确认ID为1678886452123-0的消息已处理完成
```

### XPENDING - 查看待处理消息
```
XPENDING key group [start end count] [consumer]
```

**参数详解：**
- `key`：流的名称
- `group`：消费者组名称
- `start end count`：可选，指定查看的消息范围
- `consumer`：可选，指定查看特定消费者的待处理消息

**示例：**
```
XPENDING agreementStreamQueue agreementStreamQueueGroup
# 查看agreementStreamQueueGroup中所有待处理的消息
```

### XINFO - 查看流信息
```
XINFO STREAM key
XINFO GROUPS key
XINFO CONSUMERS key groupname
```

**参数详解：**
- `STREAM`：查看流的详细信息
- `GROUPS`：查看流上的所有消费者组信息
- `CONSUMERS`：查看消费者组中的所有消费者信息

## 实际项目代码分析

### TerminalSignAgreementAgainRunner.java 代码详解

```java
@Slf4j
@Component
public class TerminalSignAgreementAgainRunner implements ApplicationRunner, DisposableBean {
    // 容器，用于管理Redis Stream消息监听
    private StreamMessageListenerContainer<String, ObjectRecord<String, String>> container;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;  // Redis操作模板
    @Autowired
    private AgreementService agreementService;        // 协议服务
    @Autowired
    private TerminalMdmService terminalMdmService;    // 终端MDM服务
    @Autowired
    private LoginUserService loginUserService;        // 登录用户服务
```

#### run() 方法详解

```java
@Override
public void run(ApplicationArguments args) {
    // 第一步：创建消费者组
    if (this.stringRedisTemplate.hasKey(TerminalSuplyConstant.STREAM_KEY)) {
        // 检查流是否存在
        StreamInfo.XInfoGroups groups = this.stringRedisTemplate.opsForStream().groups(TerminalSuplyConstant.STREAM_KEY);
        if (groups.isEmpty()) {
            // 如果流存在但没有消费者组，则创建消费者组
            this.stringRedisTemplate.opsForStream().createGroup(TerminalSuplyConstant.STREAM_KEY, TerminalSuplyConstant.STREAM_QUEUE_GROUP);
        }
    } else {
        // 如果流不存在，创建消费者组（会同时创建流）
        this.stringRedisTemplate.opsForStream().createGroup(TerminalSuplyConstant.STREAM_KEY, TerminalSuplyConstant.STREAM_QUEUE_GROUP);
    }
```

**解释：**
- `TerminalSuplyConstant.STREAM_KEY`：流的键名（"agreementStreamQueue"）
- `TerminalSuplyConstant.STREAM_QUEUE_GROUP`：消费者组名（"agreementStreamQueueGroup"）
- 这部分代码确保消费者组存在，是使用Redis Stream的前提条件

```java
    // 第二步：配置监听容器选项
    StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, String>> options =
            StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
            .pollTimeout(Duration.ofSeconds(5))      // 轮询超时时间：5秒
            .serializer(new StringRedisSerializer()) // 序列化器：字符串序列化
            .batchSize(1)                            // 批处理大小：每次处理1条消息
            .executor(Executors.newFixedThreadPool(1)) // 执行线程池：1个线程
            .targetType(String.class)                // 目标类型：字符串
            .build();
```

**解释：**
- `pollTimeout`：监听器轮询Redis的间隔时间，5秒内没有新消息会超时返回
- `batchSize`：每次从Redis获取的消息数量
- `executor`：处理消息的线程池，这里使用1个线程顺序处理消息
- 这些配置决定了消息处理的性能和行为

```java
    // 第三步：创建监听容器
    assert this.stringRedisTemplate.getConnectionFactory() != null;
    this.container = StreamMessageListenerContainer.create(this.stringRedisTemplate.getConnectionFactory(), options);
```

**解释：**
- 使用Redis连接工厂和配置选项创建监听容器
- 容器负责与Redis保持连接并监听消息

```java
    // 第四步：开始监听消息
    String uniqueConsumerName = TerminalSuplyConstant.STREAM_CONSUMER_NAME + "-" +
        java.util.UUID.randomUUID().toString().substring(0, 8);
    this.container.receiveAutoAck(
        Consumer.from(TerminalSuplyConstant.STREAM_QUEUE_GROUP, uniqueConsumerName),
        StreamOffset.create(TerminalSuplyConstant.STREAM_KEY, ReadOffset.lastConsumed()),
        new SignAgreementAgainStreamMessageListener(stringRedisTemplate, agreementService, terminalMdmService,loginUserService)
    );
```

**解释：**
- `uniqueConsumerName`：生成唯一的消费者名称，避免重复消费
- `Consumer.from()`：指定消费者组和消费者名称
- `ReadOffset.lastConsumed()`：从上次消费的位置开始读取（即只读取新的未消费消息）
- `SignAgreementAgainStreamMessageListener`：消息监听器，处理收到的消息

```java
    // 第五步：启动监听
    this.container.start();
    log.info("TerminalSignAgreementAgainRunner-->start");
}
```

**解释：**
- 启动监听容器，开始接收和处理消息
- 打印启动日志

#### SignAgreementAgainStreamMessageListener 内部类详解

```java
@Slf4j
private static class SignAgreementAgainStreamMessageListener implements StreamListener<String, ObjectRecord<String, String>> {
    // 注入的服务类
    private StringRedisTemplate stringRedisTemplate;
    private AgreementService agreementService;
    private TerminalMdmService terminalMdmService;
    private LoginUserService loginUserService;

    public SignAgreementAgainStreamMessageListener(StringRedisTemplate stringRedisTemplate, 
        AgreementService agreementService,TerminalMdmService terminalMdmService,LoginUserService loginUserService) {
        // 构造函数，初始化服务类
        this.stringRedisTemplate = stringRedisTemplate;
        this.agreementService = agreementService;
        this.terminalMdmService = terminalMdmService;
        this.loginUserService = loginUserService;
    }
```

**解释：**
- 实现 `StreamListener` 接口，用于处理收到的消息
- 保存服务类的引用，用于后续业务处理

```java
    @Override
    public void onMessage(ObjectRecord<String, String> message) {
        // 打印接收到消息的日志
        log.info("TerminalSignAgreementAgainRunner-onMessage-start:{}", JSONUtil.toJsonStr(message));
        // 将消息内容转换为TerminalDto对象
        TerminalDto eventDto = JSONUtil.toBean(message.getValue(), TerminalDto.class);
        
        try {
            // 验证终端编码不能为空
            Validate.notBlank(eventDto.getTerminalCode(),"终端编码不能为空");
            // 刷新用户认证信息
            this.loginUserService.refreshAuthentication(new Object());
            // 根据终端编码查询终端详细信息
            List<TerminalMdmVo> terminalMdmVos = this.terminalMdmService.findDetailByCodes(
                Sets.newHashSet(eventDto.getTerminalCode()));
            // 为每个终端创建协议
            for (TerminalMdmVo terminalMdmVo : terminalMdmVos) {
                this.agreementService.createByTerminalVo(terminalMdmVo);
            }
        } catch (RuntimeException e) {
            // 捕获运行时异常，记录错误日志
            log.error("TerminalSignAgreementAgainRunner处理终端供货关系变更消息时发生异常", e);
            log.info("协议签署失败，终端编码: {}", eventDto != null ? eventDto.getTerminalCode() : "未知");
        } finally {
            try {
                // 确保消息被正确删除，避免重复消费
                this.stringRedisTemplate.opsForStream().delete(TerminalSuplyConstant.STREAM_KEY, message.getId());
            } catch (Exception e) {
                log.error("删除Redis Stream消息失败: {}", message.getId(), e);
            }
        }
        log.info("TerminalSignAgreementAgainRunner-onMessage-end");
    }
}
```

**解释：**
- `onMessage`：当收到消息时调用此方法
- `JSONUtil.toBean`：将消息的JSON字符串转换为TerminalDto对象
- `Validate.notBlank`：验证终端编码不为空
- `refreshAuthentication`：刷新用户认证信息
- `findDetailByCodes`：根据终端编码查询终端详细信息
- `createByTerminalVo`：为终端创建协议
- `finally` 块：确保消息被删除，避免重复消费
- `stringRedisTemplate.opsForStream().delete`：手动删除已处理的消息

## 实际使用中的注意事项

### 消息格式设计
根据代码分析，消息的值需要能被转换为 `TerminalDto` 对象，因此发送消息时应使用JSON格式：

```bash
# 正确的格式
XADD agreementStreamQueue * terminalData "{\"terminalCode\":\"T001\",\"eventType\":\"UPDATE\"}"

# 或者直接发送JSON字符串作为值
XADD agreementStreamQueue * "{\"terminalCode\":\"T001\",\"eventType\":\"UPDATE\"}"
```

### 消费者组创建
在实际使用中，消费者组只需要创建一次，通常在应用启动时创建：

```bash
# 创建消费者组
XGROUP CREATE agreementStreamQueue agreementStreamQueueGroup $ MKSTREAM
```

### 监控命令
以下命令可用于监控和排查问题：

```bash
# 查看所有消费者组的状态信息
XINFO GROUPS agreementStreamQueue

# 查看agreementStreamQueueGroup中待处理的消息数量和时间范围
XPENDING agreementStreamQueue agreementStreamQueueGroup

# 查看具体的待处理消息详情（最多10条）
XPENDING agreementStreamQueue agreementStreamQueueGroup - + 10

# 查看消费者组中的所有消费者状态
XINFO CONSUMERS agreementStreamQueue agreementStreamQueueGroup

# 查看流的详细信息
XINFO STREAM agreementStreamQueue
```

## Key、Group 和 Consumer 的关系图解

```
Redis Server
├── Stream: agreementStreamQueue (键)
│   ├── 消息1: 1678886452123-0
│   ├── 消息2: 1678886452124-0
│   ├── 消息3: 1678886452125-0
│   └── ...
├── Consumer Group: agreementStreamQueueGroup
│   ├── Consumer A (实例1)
│   ├── Consumer B (实例2)
│   └── Consumer C (实例3)
└── Consumer Group: anotherGroup (另一个组)
    └── Consumer X
```

**关系说明：**
1. 一个 **Key（流）** 可以有多个 **Consumer Group（消费者组）**
2. 一个 **Consumer Group** 可以有多个 **Consumer（消费者）**
3. 每个 **Consumer Group** 独立跟踪消费进度
4. 同一 **Consumer Group** 内的多个 **Consumer** 共享消息（每条消息只被组内一个消费者处理）
5. 不同 **Consumer Group** 之间互不影响，都可以消费所有消息

## 常见使用场景

### 场景1：单一消费者处理
- **特点**：一个消费者组中只有一个消费者
- **用途**：需要保证消息处理顺序的场景
- **配置**：1个消费者组 + 1个消费者

### 场景2：多消费者负载均衡
- **特点**：一个消费者组中有多个消费者
- **用途**：提高消息处理能力，实现负载均衡
- **配置**：1个消费者组 + 多个消费者

### 场景3：多消费者组广播
- **特点**：多个消费者组同时消费同一消息流
- **用途**：不同的业务需要处理相同的消息
- **配置**：多个消费者组，每个组独立消费

## 最佳实践

### 1. 消费者组命名
- 使用有意义的名称，如 `agreementProcessorGroup`
- 遵循团队的命名规范

### 2. 消费者命名
- 使用唯一标识，如 `consumer-<instance-id>-<timestamp>`
- 避免消费者名称冲突

### 3. 消息确认
- 及时确认已处理的消息（XACK）
- 在 finally 块中删除消息，确保不会重复消费

### 4. 异常处理
- 捕获和记录消息处理异常
- 避免因异常导致消费者停止工作

### 5. 监控和告警
- 定期检查待处理消息数量
- 设置告警，当待处理消息数量过多时通知相关人员

### 6. 消息格式设计
- 确保消息格式与消费者代码兼容
- 推荐使用JSON格式传递复杂数据
- 保持消息格式的一致性

## 实际测试命令

```bash
# 测试发送消息（推荐格式）
XADD agreementStreamQueue * "{\"terminalCode\":\"T001\",\"eventType\":\"UPDATE\"}"

# 查看消息
XRANGE agreementStreamQueue - +

# 读取消息（使用消费者组）
XREADGROUP GROUP agreementStreamQueueGroup testConsumer COUNT 1 STREAMS agreementStreamQueue >

# 查看待处理消息
XPENDING agreementStreamQueue agreementStreamQueueGroup

# 确认消息已处理
XACK agreementStreamQueue agreementStreamQueueGroup <message-id>
```

## 错误排查指南

### 常见问题及解决方案

1. **消费者无法处理消息**
   - 检查消息格式是否正确，能否转换为预期的对象
   - 确认消费者组已正确创建

2. **消息重复消费**
   - 检查消息确认机制是否正常工作
   - 确认消息在处理完成后被正确删除或确认

3. **消费者组不存在**
   - 使用 `XINFO GROUPS <stream>` 检查消费者组是否存在
   - 如不存在，使用 `XGROUP CREATE` 命令创建

4. **待处理消息堆积**
   - 使用 `XPENDING` 检查待处理消息数量
   - 检查消费者是否正常运行
   - 考虑增加消费者实例提高处理能力
