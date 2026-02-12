# Reconciler 

使用golang实现的，mysql相同或相似表的字段和数据对比，数据分析、数据合并的通用工具。


### 安装

```shell
go get github.com/zituocn/reconciler
```

### demo

```go
package main

import "github.com/zituocn/reconciler"

func main() {
	cfg := reconciler.MergeConfig{
		DSN:           "root:123456@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4",
		TableA:        "江西-2025-招生计划",
		TableB:        "江西-2025-招生计划2",
		TableC:        "江西-2025-招生计划_test_result",
		KeyFields:     []string{"school_code", "admit", "first_sub", "major_index"},
		IgnoreFieldsA: []string{"prov", "id", "school_dm", "major_dh", "category", "type_str"},
		IgnoreFieldsB: []string{"prov", "id", "nature2", "nature", "category", "type_str", "prov", "type_str"},
		Strategy:      reconciler.UseA,
	}

	nm := reconciler.NewMerger(cfg)
	_, err := nm.Run()
	if err != nil {
		panic(err)
	}
}
```

### 结果

```shell
[开始] 数据合并任务启动 - 2026-02-12 09:59:29
[配置] A表: [江西-2025-招生计划] VS B表: [江西-2025-招生计划2] -> C表: [江西-2025-招生计划_test_result]
[配置] 关键字段: school_code,admit,first_sub,major_index
[配置] A表忽略对比字段: prov,id,school_dm,major_dh,category,type_str
[配置] B表忽略字段: prov,id,nature2,nature,category,type_str,prov,type_str
[配置] 冲突策略: 以A表为准
[信息] 数据库连接成功
[信息] A表字段(19): prov,school_code,school_dm,school_name,admit,first_sub,nature,subject_group_code,short_name,major_index,major_dh,major_name,category,number_str,charge_str,school_system_str,major_tag,second_sub,type_str
[信息] B表字段(20): admit,first_sub,nature,nature2,category,school_code,school_name,short_name,major_index,major_name,charge_str,number_str,second_sub,prov,major_tag,type_str,school_system_str,school_addr,language,tag
[信息] C表字段(19): prov,school_code,school_dm,school_name,admit,first_sub,nature,subject_group_code,short_name,major_index,major_dh,major_name,category,number_str,charge_str,school_system_str,major_tag,second_sub,type_str
[信息] 用于对比的字段(10): school_name,nature,subject_group_code,short_name,major_name,number_str,charge_str,school_system_str,major_tag,second_sub
[信息] C表(江西-2025-招生计划_test_result)已重新创建
[信息] 正在读取A表(江西-2025-招生计划)数据...
[信息] A表共 39405 条记录
[信息] 正在读取B表(江西-2025-招生计划2)数据...
[信息] B表共 38362 条记录
[信息] 开始数据对比与合并...

[冲突 #1] 关键字段 [school_code,admit,first_sub,major_index] = [0461@@本科@@物理类@@A0]
不同的字段共 1 个:

    字段[school_name]: A=山东师范大学XX                       B=山东师范大学

[待决] 以下 1 个字段两者都有值但不同，需根据策略决定:

    字段[school_name]: A=山东师范大学XX                       B=山东师范大学

    [策略] 配置为自动以A表数据为准
    [结果] 以A表数据写入C表

[冲突 #2] 关键字段 [school_code,admit,first_sub,major_index] = [0461@@本科@@历史类@@01]
不同的字段共 2 个:

    字段[major_name]: A=学前教育(师范类)是这样吗?                 B=学前教育(师范类)
    字段[charge_str]: A=5800                           B=5280

[待决] 以下 2 个字段两者都有值但不同，需根据策略决定:

    字段[major_name]: A=学前教育(师范类)是这样吗?                 B=学前教育(师范类)
    字段[charge_str]: A=5800                           B=5280

    [策略] 配置为自动以A表数据为准
    [结果] 以A表数据写入C表

[冲突 #3] 关键字段 [school_code,admit,first_sub,major_index] = [0461@@本科@@历史类@@0A]
不同的字段共 1 个:

    字段[school_system_str]: A=七年                             B=四年

[待决] 以下 1 个字段两者都有值但不同，需根据策略决定:

    字段[school_system_str]: A=七年                             B=四年

    [策略] 配置为自动以A表数据为准
    [结果] 以A表数据写入C表

[冲突 #4] 关键字段 [school_code,admit,first_sub,major_index] = [0461@@本科@@物理类@@AE]
不同的字段共 1 个:

    字段[major_name]: A=计算机科学与技术术                      B=计算机科学与技术

[待决] 以下 1 个字段两者都有值但不同，需根据策略决定:

    字段[major_name]: A=计算机科学与技术术                      B=计算机科学与技术

    [策略] 配置为自动以A表数据为准
    [结果] 以A表数据写入C表
========================================
[信息] 正在写入C表(江西-2025-招生计划_test_result)，共 40052 条记录...
[写入] 已写入 40052/40052 条记录
[完成] 数据处理任务结束 - 2026-02-12 09:59:31

========================================
           数据合并统计报告
========================================
A表总记录数:          39405
B表总记录数:          38362
C表最终记录数:        40052
----------------------------------------
完全相同记录:          37711
仅在A表中:            1690
仅在B表中:            647
关键字段相同但值不同:  4
  - 选择A表数据:      4
  - 选择B表数据:      0
自动填充空值:          0
----------------------------------------
执行耗时:              2.253709916s
========================================
```