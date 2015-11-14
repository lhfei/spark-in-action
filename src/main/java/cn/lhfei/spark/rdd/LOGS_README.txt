-----------------------------------------------------------------------------------------------------
-- 日志文件内容格式
-----------------------------------------------------------------------------------------------------
1	id	视频ID, 每个视频的唯一标识	id=01c92b9c-37c7-4510-ac87-519a1224c263
2	systemInfo	系统信息，格式为： 操作系统$是否是debug版本的flashplayer$语言$屏幕像素分辨率$水平像素$垂直像素$flashplayer版本号	systemInfo=Windows$false$zh-CN$1$1280$1024$WIN_11_6_602_180
3	ip	用户IP地址	
4	ref	视频所在页面url	ref=http://v.ifeng.com/v/news/djmdnz/index.shtml#01c92b9c-37c7-4510-ac87-519a1224c263
5	sid	注册用户的用户名，取cookie[‘sid’]	sid=3232F65C8864C995D82D087D8A15FF05kzzxc1
6	uid	访问用户的ID，用户的唯一标识	uid=1395896719356_cqf3nr8244
7	provider	视频提供商，取自XML	provider=d5f1032b-fe8b-4fbf-ab6b-601caa9480eb
8	from	引用来源，取自XML	from=niushi
9	loc	空字段	
10	cat	节目分类，取自XML	cat=0019-0052-0002
11	snapid	空字段	snapid=
12	tm	当前系统时间戳，毫秒级	tm=1424048309234
13	url	视频存储的地址	url=http://ips.ifeng.com/video19.ifeng.com/video09/2015/02/15/2999516-102-2028.mp4
14	rate	空字段	
15	dur	视频总时长，取自XML	dur=155
16	err	EventRetCode ＝ EventCode（1位）+ActionCode（2位）+Data（3位） 详见下表	err=100000
17	bt	文件总大小（B）	bt=12451187
18	bl	已加载文件大小（B）	bl=12451187
19	lt	加载文件耗时（毫秒）	lt=139059
20	se	栏目名称，取自XML	se=正点新闻
21	vid	播放器版本	vid=vNsPlayer_nsvp1.0.18
22	ptype	视频所属类型，取自xml	ptype=0019
23	cdnId	标记CDN（Sooner-赛维，Chinanet-网宿，Chinacache-蓝讯） （直播需要，非直播留空）	cdnId=ifengP2P
24	netname	运营商 （直播需要，非直播留空）	netname=移动
-----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
-- 日志文件参考标量
-----------------------------------------------------------------------------------------------------
16	err	EventRetCode ＝ EventCode（1位）+ActionCode（2位）+Data（3位） 详见下表	err=100000
3	ip	用户IP地址	
4	ref	视频所在页面url	ref=http://v.ifeng.com/v/news/djmdnz/index.shtml#01c92b9c-37c7-4510-ac87-519a1224c263
5	sid	注册用户的用户名，取cookie[‘sid’]	sid=3232F65C8864C995D82D087D8A15FF05kzzxc1
6	uid	访问用户的ID，用户的唯一标识	uid=1395896719356_cqf3nr8244
9	loc	空字段	
10  cat	节目分类，取自XML, 纪录片以'0029-'开头; 默认只分为纪录片和其他视频两类,即不以'0029-'开头的视频都归纳到'其他视频'分类中;
12	tm	当前系统时间戳，毫秒级	tm=1424048309234
13	url	视频存储的地址	url=http://ips.ifeng.com/video19.ifeng.com/video09/2015/02/15/2999516-102-2028.mp4
15	dur	视频总时长，取自XML	dur=155
17	bt	文件总大小（B）	bt=12451187
18	bl	已加载文件大小（B）	bl=12451187
19	lt	加载文件耗时（毫秒）	lt=139059
21	vid	播放器版本	vid=vNsPlayer_nsvp1.0.18
22	ptype	视频所属类型，取自xml	ptype=0019
23	cdnId	标记CDN（Sooner-赛维，Chinanet-网宿，Chinacache-蓝讯） （直播需要，非直播留空）	cdnId=ifengP2P
24	netname	运营商 （直播需要，非直播留空）	netname=移动
00  tr: time range，表示数据生成时间范围，间隔为10分钟。如10：13分生成的数据，则对应的 tr 表示为 10：20。
-----------------------------------------------------------------------------------------------------
