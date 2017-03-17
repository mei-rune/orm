# orm

对 github.com/go-xorm/xorm 的一个小封装
使用方法如下


// 全局变量定义并初始化
var Users = orm.New(func() interface{}{
  return new(User)
})


// 使用, 其中 engine 为 *xorm.Engine
Users(engine).Id(1).Get()