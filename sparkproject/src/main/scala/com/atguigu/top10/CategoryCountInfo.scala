package com.atguigu.top10

// 输出结果表
/*case class CategoryCountInfo(categoryId: String,//品类id
                             clickCount: Long,//点击次数
                             orderCount: Long,//订单次数
                             payCount: Long)//支付次数*/
//注意：样例类的属性默认是val修饰，不能修改；需要修改属性，需要采用var修饰。
// 输出结果表

case class CategoryCountInfo(var categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数

