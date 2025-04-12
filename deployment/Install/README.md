一、测试环境安装

【数据模拟环境】
1.将MACDA目录复制到/data目录下，由于/data目录未来需要存储大量数据，生产环境需要对/data挂载单独的存储空间；
命令行：cp -rp MACDA /data

2.执行fix-permission.sh，赋予数据目录适当的写权限
命令行：bash ./fix-permission.sh

3.启动数据模拟环境: 
命令行：cd /data/MACDA/mock && docker-compose up -d

4.登录https://testIP:10001,检查模拟环境kafka状态，用户名：kasm_user 密码：passw0rd
打开浏览器，连接http://mock-redpanda-console:8080/overview，查看redpanda群集状态是否正常
查看Connectors -- kafka Connector -- Tasks 下的任务File-in是否正常运行

5.启动数据模拟：
命令行：
docker exec -it mock-kafka-connect /bin/bash
cd /data/
./cpdata.sh

6.回到redpanda-console，此时应该看到Topics下Signal-in被自动创建，且会有数据产生

【测试环境】
7.启动测试环境: 
命令行：
cd /data/MACDA 
docker-compose -f docker-compose-Test.yml up -d
docker-compose -f docker-compose-Test.yml ps

8.登录https://testIP:10000,检查模拟环境kafka状态，用户名：kasm_user 密码：passw0rd
打开浏览器，连接http://redpanda-console:8080/overview，查看redpanda群集状态是否正常
查看Connectors -- kafka Connector -- Tasks 下的任务MACDA-archive-to-minio是否正常运行
如果已经启动模拟环境，应该看到Topics下signal-in被自动创建，且会有数据产生
检查 http://macda-Parse1:6166 http://macda-Store1:6166 http://macda-Status1:6166 状态是否均为 {"status":"OK"}
检查signal-parsed是否有数据

【生产环境】
7.启动测试环境: 
命令行：
cd /data/MACDA 
docker-compose -f docker-compose-Prod.yml up -d
docker-compose -f docker-compose-Prod.yml ps

8.登录https://testIP:10000,检查模拟环境kafka状态，用户名：kasm_user 密码：passw0rd
打开浏览器，连接http://redpanda-console:8080/overview，查看redpanda群集状态是否正常
查看Connectors -- kafka Connector -- Tasks 下的任务MACDA-archive-to-minio是否正常运行
如果已经启动模拟环境，应该看到Topics下signal-in被自动创建，且会有数据产生
检查 http://macda-Parse1:6166 http://macda-Store1:6166 http://macda-Status1:6166 状态是否均为 {"status":"OK"}
检查signal-parsed

【生产环境注意：】
1.在生产环境部署时不需要启动模拟环境

2.signal-in partiton数量 等于 客户kafka topic partition数量，修改signal-in partition数量时，需要先用命令：
docker-compose -f docker-compose-Prod.yml stop mirror-maker 
再在RedPanda-Console里删除signal-in，重新创建signal-in时指定需要的partition数量
再重新启动mirror-maker
docker-compose -f docker-compose-Prod.yml start mirror-maker 

3.signal-parsed partiton数量建议设置为3 且macda-Parse服务实例的数量大于等于3，修改signal-parsed partition数量时，需要先用命令：
docker-compose -f docker-compose-Prod.yml stop macda-Parse1 &&  docker-compose -f docker-compose-Prod.yml stop macda-Store1 
停掉所有的Parse和Store实例后删除signal-parsed，重新创建signal-parsed时指定需要的partition数量
再重新启动macda实例
docker-compose -f docker-compose-Prod.yml start macda-Parse1 &&  docker-compose -f docker-compose-Prod.yml start macda-Store1 

【docker-compose文件修改】
测试环境：
主要修改IP地址：192.168.32.17 修改为客户测试环境地址

生产环境：192.168.32.17 修改为客户生产环境地址

【.env文件修改】
测试环境：
主要修改IP地址：FAULT_RECORD_URL\STATS_RECORD_URL\LIFE_RECORD_URL\SYS_STATUS_URL 修改为客户上报服务器地址
生产环境：FAULT_RECORD_URL\STATS_RECORD_URL\LIFE_RECORD_URL\SYS_STATUS_URL 修改为客户上报服务器地址
同时
SEND_FAULT_RECORD=False
SEND_STATS_RECORD=False
SEND_LIFE_RECORD=False
SEND_STATUS_RECORD=False
根据需要修改为True