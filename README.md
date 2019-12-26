# cron
简易消息队列

基于workerman，结合think-worker，本版本适用于thinkphp6

# 命令
// windows
php think teu:worker cron
// linux
php think teu:worker cron start -d

备注：
  默认think-worker命令
  php think worker:server
  需要修改think-worker的命令行，增加一个参数（如cron名）
