<?php
namespace app\service\cron;
use think\worker\Server;
use think\facade\Cache;
use think\facade\Log;
use Workerman\Worker;
use Workerman\Lib\Timer;
use Workerman\Http\Client as Http;

class Cron extends Server
{
    // think-worker配置参数
    protected $protocol = 'text';
    protected $host     = '0.0.0.0';
    protected $port     = '5100';
    protected $context  = [];

    // workerman配置参数
    protected $option   = ['name'=>'cron', 'count'=>8];

    // 自定义配置
    protected $redis;   // redis驱动
    protected $http;    // 异步http组件

    protected function init()
    {
        Worker::$pidFile = app()->getRuntimePath() .'pid/worker_'. $this->port .'.pid';
        Worker::$logFile = app()->getRuntimePath() .'log/worker_'. $this->port .'.log';
    }

    public function onWorkerStart($worker)
    {
        // 事例化HTTP组件
        $this->http  = new Http();
        $this->redis = Cache::store('redis');

        // 添加定时器 - schedule
        Timer::add(3, function() {
            $time = time();
            while (($time = $this->nextDelay($time)) !== false) {
                $job = null;
                while ($job = $this->nextJob($time)) {
                    call_user_func_array([$this, 'crontab'], [$job['name'], $job['args'], $time, $job['queue'], $job['count'], true]);
                }
            }
        });
        // 添加定时器 - consumer
        Timer::add(1, function() {
            // 任务队列处理
            while(1) {
                if ($name = $this->redis->lpop('teu:queue')) {
                    while (1) {
                        if ($job = $this->redis->lpop('teu:queue:'. $name)) {
                            $job = json_decode($job, true);
                            if (isset($job['name'])) {
                                try {
                                    // 远程调用
                                    if (strpos($job['name'], 'http') === 0) {
                                        // 异步处理
                                        $this->http->post($job['name'], $job['args'], function($res) use ($name, $job) {
                                            $res->getBody() == 'success' || $this->retry($name, $job);
                                        }, function($e) use ($name, $job) {
                                            $this->retry($name, $job, $e->getMessage());
                                        });
                                    } else {
                                        // 回调处理
                                        $callback = explode('/', $job['name']);
                                        strpos($callback[0], '\\') === 0 || $callback[0] = __NAMESPACE__ .'\\job\\'. $callback[0];
                                        // 去掉名称中的TAG标记
                                        $callback[1] = isset($callback[1]) ? preg_replace('/:\w+/', '', $callback[1]) : 'perform';
                                        // 事例化
                                        $callback[0] = new $callback[0];
                                        // 尝试执行
                                        if (is_callable($callback)) {
                                            call_user_func_array([$callback[0], $callback[1]], [$job['args']]) === true || $this->retry($name, $job);
                                        }
                                    }
                                } catch (\Exception $e) {
                                    $this->retry($name, $job, $e->getMessage());
                                }
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        });
    }

    public function onMessage($conn, $data)
    {
        $data = json_decode($data, true);
        // 写入任务队列
        if (isset($data['name']) && isset($data['time']) && isset($data['queue']) && isset($data['count'])) {
            call_user_func_array([$this, 'crontab'], [$data['name'], $data['args'], $data['time'], $data['queue'], $data['count']]);
            $conn->send('success');
        } else {
            $conn->send('error');
        }
    }

    // 重试任务计划
    protected function retry($name, $job, $error = null)
    {
        if (count($job['count']) > 0) {
            // 重新写入队列
            call_user_func_array([$this, 'crontab'], [
                $job['name'],
                $job['args'],
                array_pop($job['count']),
                $name,
                $job['count']
            ]);
            // 记录异常
            isset($error) && Log::channel('cron')->error($error);
        } else {
            // 记录失败
            Log::channel('cron')->info(['cron failed', $name, $job]);
        }
    }

    // 下一个延迟
    protected function nextDelay($time)
    {
        $time = $this->redis->zrangebyscore('teu:delay', '-inf', $time, ['limit'=>[0, 1]]);
        if (!empty($time)) {
            return $time[0];
        }
        return false;
    }

    // 下一个任务
    protected function nextJob($time)
    {
        $key = 'teu:delay:'. $time;
        $job = json_decode($this->redis->lpop($key), true);
        // 删除计划
        if ($this->redis->llen($key) == 0) {
            $this->redis->del($key);
            $this->redis->zrem('teu:delay', $time);
        }
        return $job;
    }

    // 写入计划
    protected function crontab($name, $args = null, $time = 0, $queue = 'default', $count = [], $delay = false)
    {
        // 取消定时任务
        if ($args === false) {
            $this->clear($name, $time);
        } else {
            if ($time > time()) {
                $data = [
                    'name'  => $name,
                    'args'  => $args,
                    'queue' => $queue,
                    'count' => $count   // 重试次数
                ];
                $this->redis->rpush('teu:delay:'. $time, json_encode($data, 320));
                $this->redis->zadd('teu:delay', $time, $time);
                $this->redis->sadd('teu:delay:'. md5($name), $time);
            } else {
                // 清除标记
                $delay && $this->redis->srem('teu:delay:'. md5($name), $time);
                // 写入任务队列数据
                $data = [
                    'name'  => $name,
                    'args'  => $args,
                    'count' => $count   // 重试次数
                ];
                // 队列名插入队列尾部
                $this->redis->rpush('teu:queue', $queue);
                // 任务插入指定队列尾部
                $this->redis->rpush('teu:queue:'. $queue, json_encode($data, 320));
            }
        }
    }

    // 清除计划
    protected function clear($name, $time)
    {
        $name = md5($name);
        if ($time > 0) {
            $this->redis->ltrim('teu:delay:'. $time, 1, 0);
            $this->redis->zrem('teu:delay', $time);
            $this->redis->srem('teu:delay:'. $name, $time);
        } else {
            $delay = $this->redis->smembers('teu:delay:'. $name);
            foreach ($delay as $time) {
                $this->redis->ltrim('teu:delay:'. $time, 1, 0);
                $this->redis->zrem('teu:delay', $time);
            }
            $this->redis->del('teu:delay:'. $name);
        }
    }
}
