package main

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	model "dscgs/v2-grpc/model"
	pb_gen "dscgs/v2-grpc/service/shorturl_generation_service"
	database "dscgs/v2-grpc/utils/database"
	redisutil "dscgs/v2-grpc/utils/redis"
	redis "github.com/redis/go-redis/v9"
	idgenerator "dscgs/v2-grpc/utils/idgenerator"
	number "dscgs/v2-grpc/utils/number"
	ch "dscgs/v2-grpc/utils/consistenthash"

	"google.golang.org/grpc"
	"gorm.io/gorm"
)

const (
	defaultRedisAddr = "localhost:6379"
	base62Length = 7
	originalUrlKeyPrefix = "long:"
	shortUrlKeyPrefix = "short:"
	redisShortUrlExpireHours = 24 // 短链在Redis中的过期时间
	distributedLockPrefix = "genlock:long:"
	distributedLockTTL = 2 * time.Second
	lockWaitTimeout = 1 * time.Second
	lockPollInterval = 100 * time.Millisecond
	maxShortUrlGenerationAttempts = 5
	bloomFilterName = "GeneratedOriginalUrlBF"
)

var (
	// 用62进制的1位来表示库号，1位表示表号，因此库号数组和表号数据的元素值都只能是数字或者字母（string形式）
	dbIDs []string = []string{"a", "b"} // 两个库
	tableIDs []string = []string{"j", "k"} // 两个表
)

// 静态配置
type ServerConfig struct {
	DB *gorm.DB
	HashringDB *ch.HashRing
	HashringDBTable *ch.HashRing
	RedisAddr string
	// WorkerNodeID int64
}

// 动态创建
type generationServer struct { // 这里可以都用小写
	pb_gen.UnimplementedShortURLGenerationServiceServer // 嵌入，不是字段！不可以是server pb....
	redisUtils *redisutil.RedisUtils // 共享同一个redisUtil实例
	cfg ServerConfig
	// idWorker *idgenerator.Worker
}

func NewGenerationServer(cfg ServerConfig) *generationServer {
	// 验证配置
	// TODO

	// 创建Redis客户端
	var redisAddr string
	if cfg.RedisAddr == "" {
		redisAddr = defaultRedisAddr
	} else{
		redisAddr = cfg.RedisAddr
	}
	redisUtils := &redisutil.RedisUtils{ServerAddr: redisAddr}

	// 创建服务器实例
	server := &generationServer {
		cfg: cfg,
		redisUtils: redisUtils,
	}

	return server
}


func (gs *generationServer) GenerateShortURL(ctx context.Context, req *pb_gen.GenerateShortURLRequest) (*pb_gen.GenerateShortURLResponse, error) {
	originalUrl := req.OriginalUrl
	shortUrl := ""
	var err error = nil

	// ------ 检查Redis是否已经存在生成的短链 ------
	key := originalUrlKeyPrefix + originalUrl
	if result, exists := gs.redisUtils.GetKey(ctx, key); exists {
		// ------- Redis里已经存有当前长链的信息 -------
		// 检查短链是否过期
		if isExpired := gs.redisUtils.IsExpired(ctx, key); isExpired {
			// 短链已经过期，则查询数据库
			// 删除Redis中已经过期的短链
			shortUrlKey := shortUrlKeyPrefix + result.(string)
			gs.redisUtils.DeleteKey(ctx, shortUrlKey)
			shortUrl = gs.getShortUrlFromDB(ctx, originalUrl)
		} else {
			shortUrl = result.(string) // 类型断言，将any类型的result转换为string
		}
	} else {
		// ------- Redis里不存在当前长链的键 -------
		log.Println("Redis里不存在", key, "查询布隆过滤器......")
		// 检查布隆过滤器
		if exists = gs.redisUtils.BFExists(ctx, bloomFilterName, originalUrl); exists {
			// 布隆过滤器里存在当前长链，访问数据库
			shortUrl = gs.getShortUrlFromDB(ctx, originalUrl)
		} else {
			// 布隆过滤器里不存在当前长链，则数据库中也必然不存在长链信息
			// 生成短链并返回
			shortUrl = gs.createShortURL(ctx, originalUrl)
		}

	}

	return &pb_gen.GenerateShortURLResponse{ShortUrl: shortUrl}, err
}

// 👇🏻 访问MySQL，看长链是否存在+是否已经过期
func (gs *generationServer) getShortUrlFromDB(ctx context.Context, originalUrl string) (shortUrl string) {
	var mapping model.URLMapping
	dbError := gs.cfg.DB.Where("original_url = ?", originalUrl).First(&mapping).Error
	if errors.Is(dbError, gorm.ErrRecordNotFound){
		// 数据库中不存在当前长链
		log.Printf("查询不到长链%v", originalUrl)
		shortUrl = gs.createShortURL(ctx, originalUrl)
	} else if dbError != nil {
		log.Fatalf("数据库查询失败: %v", dbError) // TODO: 这里有问题
		panic(dbError)
	} else{
		// 数据库中存在当前长链，需要检查是否过期
		expireTime := mapping.ExpireAt
		if expireTime.After(time.Now()) {
			// 数据库中的短链已经过期
			shortUrl = gs.createShortURL(ctx, originalUrl)
		} else {
			// 数据库中的短链没有过期
			// 更新Redis，添加当前长短链对应关系，设置数据库中写定的过期时间
			gs.redisUtils.AddKeyEx(ctx, originalUrlKeyPrefix + originalUrl, shortUrl, time.Until(mapping.ExpireAt).Hours())
			gs.redisUtils.AddKeyEx(ctx, shortUrlKeyPrefix + shortUrl, originalUrl, time.Until(mapping.ExpireAt).Hours())
			shortUrl = mapping.ShortUrl
		}
	}
	return shortUrl
}

// 👇🏻 真正的开始生成短链的逻辑
func (gs *generationServer) createShortURL(ctx context.Context, originalUrl string) (shortUrl string) {
	redisClient := gs.redisUtils.GetRedisClient()
	// 获取分布式锁
	keyLock := distributedLockPrefix + originalUrl
	valueLock := "" // uuid
	ok, err := redisClient.SetNX(context.Background(), keyLock, valueLock, distributedLockTTL).Result()
	if err != nil {
		log.Fatalf("获取分布式锁%v时产生错误: %v", keyLock, err)
		panic(err)
	}
	if ok {
		// 当前goroutine成功获取到分布式锁
		// 确保锁的释放，使用lua脚本保证原子性（redis执行指令是单线程的）
		var luaScript = redis.NewScript(`
			local value = redis.call("Get", KEYS[keyLock])
			if (value == valueLock) then
				redis.call("Del", KEYS[keyLock])
			end
		`)
		defer luaScript.Run(context.Background(), redisClient, []string{keyLock}) // 需要错误检测吗

		// 利用雪花算法，生成64位id，并编码为62进制，取7位，并在首位添加1位库号，末尾添加1位表号
		snowFlakeID := getSnowflakeID()
		snowFlakeID62 := number.DecimalToBase62(snowFlakeID, base62Length)
		shortUrl = gs.formIDToShortUrl(snowFlakeID62, 1, 1)

		// 将长短链映射关系写入数据库，根据短链唯一索引，检查是否已经存在短链，是否需要重新生成
		mapping := model.URLMapping{
			OriginalUrl: originalUrl,
			ShortUrl: shortUrl,
			ExpireAt: time.Now().Add(redisShortUrlExpireHours * time.Hour),
			AccessCount: 0,
		}
		dbError := gs.cfg.DB.Create(&mapping).Error
		if dbError != nil {
			// 插入失败，可能是短链重复了
			log.Printf("插入长短链映射关系失败，可能是短链重复，错误信息: %v", dbError)
			shortUrl = gs.createShortURL(ctx, originalUrl) // 递归调用，重新生成短链
		} else {
			// 插入成功
			log.Printf("插入长短链映射关系成功，长链: %v, 短链: %v", originalUrl, shortUrl)
			// 将当前长短链对应关系写入布隆过滤器和Redis
			gs.redisUtils.BFAdd(ctx, bloomFilterName, originalUrl)
			// 设置短链在Redis中的过期时间
			gs.redisUtils.AddKeyEx(ctx, originalUrlKeyPrefix +originalUrl, shortUrl, redisShortUrlExpireHours)
			gs.redisUtils.AddKeyEx(ctx, shortUrlKeyPrefix+shortUrl, originalUrl, redisShortUrlExpireHours)
		}
	} else {
		// 当前goroutine没有获取到分布式锁
		// 挂起，等待一个timeout，如果timeout结束前还没有等到锁的释放信息，就返回空，提示无法分享，稍后再试。如果等到了，就再查一下Redis。
		timeout := time.After(lockWaitTimeout)
		ticker := time.NewTicker(lockPollInterval)

		defer ticker.Stop()

		for {
			select {
			case <- timeout:
				log.Printf("等待分布式超时，长链：%v", originalUrl)
				return ""
			case <- ticker.C:
				// 检查锁是否已经释放
				lockExists, err := redisClient.Exists(context.Background(), keyLock).Result()
				if err != nil {
					log.Fatalf("检查锁状态失败: %v", err)
					// return ""
				}
				if lockExists == 0 {
					// 锁已经释放，重新尝试获取短链
					if existingShortUrl, err := redisClient.Get(context.Background(), originalUrlKeyPrefix +originalUrl).Result(); err == nil {
						log.Printf("等待后获取到已经存在的短链：%v", existingShortUrl)
						return existingShortUrl
					}
				// 重新尝试生成短链
				return gs.createShortURL(ctx, originalUrl)
				}
			}
		}
	}

	return ""
}

// 用雪花算法生成id
func getSnowflakeID() int64 {
	// 假设当前是workerID=1的机器
	workerID := 1
	worker, err := idgenerator.NewWorker(int64(workerID))
	if err != nil {
		log.Fatalf("创建worker失败: %v", err)
		panic(err)
	}
	snowflakeID := worker.GetID()
	return snowflakeID
}


// 在编码后的id前后添加库号和表号，库位数和表位数指定，用于分库分表
func (gs *generationServer) formIDToShortUrl(str62 string, lenDB int, lenTable int) (shortUrl string) {
	// 计算库号和表号
	dbID := gs.cfg.HashringDB.GetNode(str62).Name // 这里的Name就是id
	tableID := gs.cfg.HashringDBTable.GetNode(str62).Name
	// 使用一致性哈希算法，计算库号和表号
	shortUrl = dbID + str62 + tableID
	return shortUrl
}

func main() {
	// ------------------------------------------------------
	// 建立数据库连接
	// ------------------------------------------------------
	dbConfig := database.DBConfig{
		Username: "dongying",
		Password: "my_password",
		Host: "localhost",
		Port: 3306,
		DBName: "shorturl_db",
		LogMode: true, // 开启日志模式
	}

	db, err := database.NewDBConnection(dbConfig)

	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}

	if err := database.AutoMigrate(db); err != nil {
		log.Fatalf("数据库自动迁移失败: %v", err)
	}

	// ------------------------------------------------------
	//  初始化哈希环，库和表各有一个哈希环
	// ------------------------------------------------------
	hashringDB := ch.NewHashRing(1<<32) // 哈希环长度2的32次方
	// 初始化数据库物理节点
	dbNode1 := &ch.PhysicalServerNode{
		Node: ch.Node{Name: dbIDs[0]},
		Weight: ch.WEIGHT_2,
	}
	dbNode2 := &ch.PhysicalServerNode{
		Node: ch.Node{Name: dbIDs[1]},
		Weight: ch.WEIGHT_DEFAULT,
	}
	hashringDB.AddPhysicalServerNode(dbNode1)
	hashringDB.AddPhysicalServerNode(dbNode2)

	hashringDBTable := ch.NewHashRing(1<<32)

	// 初始化表物理节点
	tableNode1 := &ch.PhysicalServerNode{
		Node: ch.Node{Name: tableIDs[0]},
		Weight: ch.WEIGHT_DEFAULT,
	}
	tableNode2 := &ch.PhysicalServerNode{
		Node: ch.Node{Name: tableIDs[1]},
		Weight: ch.WEIGHT_2,
	}
	hashringDBTable.AddPhysicalServerNode(tableNode1)
	hashringDBTable.AddPhysicalServerNode(tableNode2)

	// ------------------------------------------------------
	// 创建一个新的gRPC服务器实例
	// ------------------------------------------------------
	grpcServer := grpc.NewServer() // 固定搭配
	// 创建短链注册服务器，并注入依赖（数据库连接）
	cfg := ServerConfig{
		DB: db,
		HashringDB: hashringDB,
		HashringDBTable: hashringDBTable,
		RedisAddr: defaultRedisAddr, // TODO: 这个可以改为从配置文件读取
		// WorkerNodeID: 1,
	}

	generationServer := NewGenerationServer(cfg)
	// 注册短链生成服务到gRPC服务器
	pb_gen.RegisterShortURLGenerationServiceServer(grpcServer, generationServer)

	// ------------------------------------------------------
	// 启动grpc监听
	// ------------------------------------------------------
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("无法监听端口: %v", err)
	}

	// 启动服务器
	log.Printf("服务器监听端口 %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("无法启动服务器: %v", err)
	}

}

// gRPC 服务器的工作方式：
// 1. main() 执行一次，启动服务
// 2. 对于每个传入的请求，gRPC 会：
//    - 创建新的 goroutine 处理请求 ✅
//    - 调用对应的 handler 方法 ✅
//    - 所有 handler 共享同一个 server 实例 ✅
