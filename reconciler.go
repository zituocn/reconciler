package reconciler

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/zituocn/logx"
)

// ConflictStrategy 冲突处理策略
type ConflictStrategy int

const (
	// UseA 以A表数据为准
	UseA ConflictStrategy = iota
	// UseB 以B表数据为准
	UseB
	// AskUser 交互式询问用户
	AskUser
)

// MergeConfig 合并配置
type MergeConfig struct {
	// 数据库连接字符串，例如 "user:password@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=true"
	DSN string

	// A表名称（主表）
	TableA string
	// B表名称
	TableB string
	// C表名称（输出结果表）
	TableC string

	// 多个关键字段名称，用于判断是否为同一条数据
	KeyFields []string

	// A表中忽略对比的字段（其值仍然写入C表）
	IgnoreFieldsA []string
	// B表中忽略的字段（其值不参与对比，也不写入C表）
	IgnoreFieldsB []string

	// 冲突处理策略：当关键字段相同但其他字段不同时
	Strategy ConflictStrategy

	// 批量写入大小
	BatchSize int
}

// MergeStats 合并统计信息
type MergeStats struct {
	TotalA         int // A表总记录数
	TotalB         int // B表总记录数
	TotalC         int // C表最终记录数
	ExactMatch     int // 完全相同的记录数
	OnlyInA        int // 仅在A表中的记录数
	OnlyInB        int // 仅在B表中的记录数
	Conflict       int // 关键字段相同但其他字段不同的记录数
	NullAutoFilled int // 自动用非空值填充的记录数
	ConflictUseA   int // 冲突中选择A的次数
	ConflictUseB   int // 冲突中选择B的次数
	StartTime      time.Time
	EndTime        time.Time
}

// String 返回统计信息的可读字符串
func (s *MergeStats) String() string {
	duration := s.EndTime.Sub(s.StartTime)
	return fmt.Sprintf(`
========================================
           数据合并统计报告
========================================
A表总记录数:          %d
B表总记录数:          %d
C表最终记录数:        %d
----------------------------------------
完全相同记录:          %d
仅在A表中:            %d
仅在B表中:            %d
关键字段相同但值不同:  %d
  - 选择A表数据:      %d
  - 选择B表数据:      %d
自动填充空值:          %d
----------------------------------------
执行耗时:              %v
========================================
`, s.TotalA, s.TotalB, s.TotalC,
		s.ExactMatch, s.OnlyInA, s.OnlyInB,
		s.Conflict, s.ConflictUseA, s.ConflictUseB,
		s.NullAutoFilled, duration)
}

// columnInfo 列信息
type columnInfo struct {
	Name            string
	OrdinalPosition int
	ColumnDefault   sql.NullString
	IsNullable      string
	DataType        string
	ColumnType      string
	Extra           string
	FullDefinition  string // 完整的列定义，用于创建表
}

// rowData 行数据，所有值存为 *string（nil 表示 NULL）
type rowData struct {
	Values map[string]*string
}

// Merger 数据合并器
type Merger struct {
	config MergeConfig
	db     *sql.DB
	stats  MergeStats

	columnsA    []columnInfo // A表的列信息（排除id）
	columnsB    []columnInfo // B表的列信息（排除id）
	columnsC    []columnInfo // C表的列信息（以A表为准）
	fieldNamesA []string     // A表字段名列表
	fieldNamesB []string     // B表字段名列表
	fieldNamesC []string     // C表字段名列表

	ignoreSetA map[string]bool // A表忽略字段集合
	ignoreSetB map[string]bool // B表忽略字段集合

	// 用于对比的字段：C表字段中排除关键字段和A忽略字段
	compareFields []string

	// B表字段在C表中存在的映射
	bFieldInC map[string]bool

	// 标准输入读取器（全局唯一，避免重复创建导致缓冲区混乱）
	stdinReader *bufio.Reader
}

// NewMerger 创建新的合并器
func NewMerger(config MergeConfig) *Merger {
	if config.BatchSize <= 0 {
		config.BatchSize = 500
	}
	m := &Merger{
		config:      config,
		ignoreSetA:  make(map[string]bool),
		ignoreSetB:  make(map[string]bool),
		bFieldInC:   make(map[string]bool),
		stdinReader: bufio.NewReader(os.Stdin), // 只创建一次
	}
	for _, f := range config.IgnoreFieldsA {
		m.ignoreSetA[f] = true
	}
	for _, f := range config.IgnoreFieldsB {
		m.ignoreSetB[f] = true
	}
	return m
}

// Run 执行合并操作
func (m *Merger) Run() (*MergeStats, error) {
	m.stats = MergeStats{} // 重置统计
	m.stats.StartTime = time.Now()
	fmt.Printf("[开始] 数据合并任务启动 - %s\n", m.stats.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("[配置] A表: [%s] VS B表: [%s] -> C表: [%s]\n", m.config.TableA, m.config.TableB, m.config.TableC)
	fmt.Printf("[配置] 关键字段: %v\n", strings.Join(m.config.KeyFields, ","))
	if len(m.config.IgnoreFieldsA) > 0 {
		fmt.Printf("[配置] A表忽略对比字段: %v\n", strings.Join(m.config.IgnoreFieldsA, ","))
	}
	if len(m.config.IgnoreFieldsB) > 0 {
		fmt.Printf("[配置] B表忽略字段: %v\n", strings.Join(m.config.IgnoreFieldsB, ","))
	}
	strategyName := "以A表为准"
	if m.config.Strategy == UseB {
		strategyName = "以B表为准"
	} else if m.config.Strategy == AskUser {
		strategyName = "交互式询问用户"
	}
	fmt.Printf("[配置] 冲突策略: %s\n", strategyName)

	// 1. 连接数据库
	var err error
	m.db, err = sql.Open("mysql", m.config.DSN)
	if err != nil {
		logx.Errorf("连接数据库失败: %v", err)
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}
	defer m.db.Close()

	if err = m.db.Ping(); err != nil {
		logx.Errorf("数据库Ping失败: %v", err)
		return nil, fmt.Errorf("数据库Ping失败: %v", err)
	}
	fmt.Printf("[信息] 数据库连接成功\n")

	// 2. 获取A表和B表的列信息
	m.columnsA, err = m.getColumns(m.config.TableA)
	if err != nil {
		return nil, err
	}
	m.columnsB, err = m.getColumns(m.config.TableB)
	if err != nil {
		return nil, err
	}

	// 重置字段名列表
	m.fieldNamesA = nil
	m.fieldNamesB = nil
	m.fieldNamesC = nil
	m.compareFields = nil

	for _, c := range m.columnsA {
		m.fieldNamesA = append(m.fieldNamesA, c.Name)
	}
	for _, c := range m.columnsB {
		m.fieldNamesB = append(m.fieldNamesB, c.Name)
	}

	// 3. C表字段以A表为准
	m.columnsC = make([]columnInfo, len(m.columnsA))
	copy(m.columnsC, m.columnsA)
	for _, c := range m.columnsC {
		m.fieldNamesC = append(m.fieldNamesC, c.Name)
	}

	// 构建B表字段集合，判断B表字段是否在C表中
	bFieldSet := make(map[string]bool)
	for _, f := range m.fieldNamesB {
		bFieldSet[f] = true
	}
	for _, f := range m.fieldNamesC {
		if bFieldSet[f] {
			m.bFieldInC[f] = true
		}
	}

	// 构建用于对比的字段列表：C表字段中排除关键字段和A表忽略字段
	keySet := make(map[string]bool)
	for _, k := range m.config.KeyFields {
		keySet[k] = true
	}
	for _, f := range m.fieldNamesC {
		if !keySet[f] && !m.ignoreSetA[f] {
			m.compareFields = append(m.compareFields, f)
		}
	}

	fmt.Printf("[信息] A表字段(%d): %v\n", len(m.fieldNamesA), strings.Join(m.fieldNamesA, ","))
	fmt.Printf("[信息] B表字段(%d): %v\n", len(m.fieldNamesB), strings.Join(m.fieldNamesB, ","))
	fmt.Printf("[信息] C表字段(%d): %v\n", len(m.fieldNamesC), strings.Join(m.fieldNamesC, ","))
	fmt.Printf("[信息] 用于对比的字段(%d): %v\n", len(m.compareFields), strings.Join(m.compareFields, ","))

	// 4. 重新创建C表
	if err = m.recreateTableC(); err != nil {
		return nil, err
	}

	// 5. 读取A表数据
	fmt.Printf("[信息] 正在读取A表(%s)数据...\n", m.config.TableA)
	dataA, err := m.readTable(m.config.TableA, m.fieldNamesA)
	if err != nil {
		return nil, err
	}
	m.stats.TotalA = len(dataA)
	fmt.Printf("[信息] A表共 %d 条记录\n", m.stats.TotalA)

	// 6. 读取B表数据
	fmt.Printf("[信息] 正在读取B表(%s)数据...\n", m.config.TableB)
	dataB, err := m.readTable(m.config.TableB, m.fieldNamesB)
	if err != nil {
		return nil, err
	}
	m.stats.TotalB = len(dataB)
	fmt.Printf("[信息] B表共 %d 条记录\n", m.stats.TotalB)

	// 7. 建立B表索引：key -> rowData
	bIndex := make(map[string]*rowData)
	for i := range dataB {
		key := m.buildKey(&dataB[i])
		bIndex[key] = &dataB[i]
	}

	// 8. 对比并合并
	fmt.Printf("[信息] 开始数据对比与合并...\n")
	var resultRows []rowData
	bMatched := make(map[string]bool) // 记录B表中已匹配的key

	for i := range dataA {
		rowA := &dataA[i]
		keyA := m.buildKey(rowA)

		if rowB, ok := bIndex[keyA]; ok {
			// 在B表中找到了相同关键字段的记录
			bMatched[keyA] = true
			merged := m.compareAndMerge(rowA, rowB, keyA)
			resultRows = append(resultRows, *merged)
		} else {
			// 仅在A表中
			m.stats.OnlyInA++
			resultRows = append(resultRows, *m.buildCRowFromAWithMeta(rowA, "A", false, ""))
		}
	}

	// 9. 处理仅在B表中的数据
	for i := range dataB {
		key := m.buildKey(&dataB[i])
		if !bMatched[key] {
			m.stats.OnlyInB++
			resultRows = append(resultRows, *m.buildCRowFromB(&dataB[i]))
		}
	}

	// 10. 批量写入C表
	fmt.Printf("========================================\n")
	fmt.Printf("[信息] 正在写入C表(%s)，共 %d 条记录...\n", m.config.TableC, len(resultRows))
	if err = m.batchInsertC(resultRows); err != nil {
		return nil, err
	}
	m.stats.TotalC = len(resultRows)

	m.stats.EndTime = time.Now()
	fmt.Printf("[完成] 数据处理任务结束 - %s\n", m.stats.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Print(m.stats.String())

	return &m.stats, nil
}

// getColumns 获取表的列信息（排除自增主键id）
func (m *Merger) getColumns(tableName string) ([]columnInfo, error) {
	query := `
		SELECT 
			COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE,
			DATA_TYPE, COLUMN_TYPE, EXTRA
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := m.db.Query(query, tableName)
	if err != nil {
		logx.Errorf("查询表%s列信息失败: %v", tableName, err)
		return nil, fmt.Errorf("查询表%s列信息失败: %v", tableName, err)
	}
	defer rows.Close()

	var columns []columnInfo
	for rows.Next() {
		var col columnInfo
		if err := rows.Scan(&col.Name, &col.OrdinalPosition, &col.ColumnDefault,
			&col.IsNullable, &col.DataType, &col.ColumnType, &col.Extra); err != nil {
			logx.Errorf("扫描列信息失败: %v", err)
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}
		// 排除自增主键id
		if strings.ToLower(col.Name) == "id" && strings.Contains(strings.ToLower(col.Extra), "auto_increment") {
			continue
		}
		// 构建完整列定义
		col.FullDefinition = m.buildColumnDef(col)
		columns = append(columns, col)
	}
	if err = rows.Err(); err != nil {
		logx.Errorf("遍历列信息出错: %v", err)
		return nil, fmt.Errorf("遍历列信息出错: %v", err)
	}
	if len(columns) == 0 {
		logx.Errorf("表%s没有找到列（或表不存在）", tableName)
		return nil, fmt.Errorf("表%s没有找到列（或表不存在）", tableName)
	}
	return columns, nil
}

// buildColumnDef 构建列的DDL定义（C表中所有字段都允许NULL）
func (m *Merger) buildColumnDef(col columnInfo) string {
	def := fmt.Sprintf("`%s` %s", col.Name, col.ColumnType)
	// C表中所有字段都允许为空（因为B表写入时可能缺少字段）
	def += " NULL"
	if col.ColumnDefault.Valid {
		def += fmt.Sprintf(" DEFAULT '%s'", col.ColumnDefault.String)
	} else {
		def += " DEFAULT NULL"
	}
	return def
}

// recreateTableC 重新创建C表
func (m *Merger) recreateTableC() error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", m.config.TableC)
	if _, err := m.db.Exec(dropSQL); err != nil {
		logx.Errorf("删除C表失败: %v", err)
		return fmt.Errorf("删除C表失败: %v", err)
	}

	var colDefs []string
	colDefs = append(colDefs, "`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY")
	for _, col := range m.columnsC {
		colDefs = append(colDefs, col.FullDefinition)
	}
	// 添加来源标记字段和冲突标记字段
	colDefs = append(colDefs, "`_source` VARCHAR(10) NULL DEFAULT NULL COMMENT '数据来源: A/B/MERGE_A/MERGE_B'")
	colDefs = append(colDefs, "`_conflict` TINYINT(1) NULL DEFAULT 0 COMMENT '是否冲突记录: 0-否, 1-是'")
	colDefs = append(colDefs, "`_diff_fields` TEXT NULL DEFAULT NULL COMMENT '不同的字段列表'")

	createSQL := fmt.Sprintf("CREATE TABLE `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		m.config.TableC, strings.Join(colDefs, ",\n  "))

	if _, err := m.db.Exec(createSQL); err != nil {
		logx.Errorf("创建C表失败: %v\nSQL: %s", err, createSQL)
		return fmt.Errorf("创建C表失败: %v", err)
	}
	fmt.Printf("[信息] C表(%s)已重新创建\n", m.config.TableC)
	return nil
}

// readTable 读取表的所有数据
func (m *Merger) readTable(tableName string, fieldNames []string) ([]rowData, error) {
	quotedFields := make([]string, len(fieldNames))
	for i, f := range fieldNames {
		quotedFields[i] = fmt.Sprintf("`%s`", f)
	}
	query := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(quotedFields, ", "), tableName)
	rows, err := m.db.Query(query)
	if err != nil {
		logx.Errorf("查询表%s数据失败: %v", tableName, err)
		return nil, fmt.Errorf("查询表%s数据失败: %v", tableName, err)
	}
	defer rows.Close()

	var result []rowData
	for rows.Next() {
		scanArgs := make([]interface{}, len(fieldNames))
		nullStrings := make([]sql.NullString, len(fieldNames))
		for i := range scanArgs {
			scanArgs[i] = &nullStrings[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			logx.Errorf("扫描数据行失败: %v", err)
			return nil, fmt.Errorf("扫描数据行失败: %v", err)
		}
		rd := rowData{Values: make(map[string]*string)}
		for i, f := range fieldNames {
			if nullStrings[i].Valid {
				val := nullStrings[i].String
				rd.Values[f] = &val
			} else {
				rd.Values[f] = nil // NULL
			}
		}
		result = append(result, rd)
	}
	if err = rows.Err(); err != nil {
		logx.Errorf("遍历数据出错: %v", err)
		return nil, fmt.Errorf("遍历数据出错: %v", err)
	}
	return result, nil
}

// buildKey 根据关键字段构建唯一key
func (m *Merger) buildKey(row *rowData) string {
	parts := make([]string, len(m.config.KeyFields))
	for i, kf := range m.config.KeyFields {
		val := row.Values[kf]
		if val == nil {
			parts[i] = "\x00<NULL>\x00"
		} else {
			parts[i] = *val
		}
	}
	return strings.Join(parts, "\x01@@\x01")
}

// compareAndMerge 比较两行数据并合并
func (m *Merger) compareAndMerge(rowA, rowB *rowData, key string) *rowData {
	// 第一遍：找出所有不同的字段
	var diffFields []string

	for _, f := range m.compareFields {
		// B表中忽略的字段不参与对比
		if m.ignoreSetB[f] {
			continue
		}
		valA := rowA.Values[f]
		// B表中可能没有此字段
		valB, bHasField := rowB.Values[f]
		if !bHasField {
			continue
		}
		if !valuesEqual(valA, valB) {
			diffFields = append(diffFields, f)
		}
	}

	// 完全相同
	if len(diffFields) == 0 {
		m.stats.ExactMatch++
		return m.buildCRowFromAWithMeta(rowA, "A", false, "")
	}

	// 有差异，打印冲突信息
	m.stats.Conflict++
	fmt.Printf("\n[冲突 #%d] 关键字段 [%v] = [%s]\n", m.stats.Conflict, strings.Join(m.config.KeyFields, ","), key)
	fmt.Printf("不同的字段共 %d 个:\n\n", len(diffFields))
	for _, f := range diffFields {
		aVal := displayValue(rowA.Values[f])
		bVal := "<字段不存在>"
		if v, ok := rowB.Values[f]; ok {
			bVal = displayValue(v)
		}
		fmt.Printf("    字段[%s]: A=%-30s B=%s\n", f, aVal, bVal)
	}

	// 第二遍：构建合并行，先以A为基础
	merged := &rowData{Values: make(map[string]*string)}
	for _, f := range m.fieldNamesC {
		if v, ok := rowA.Values[f]; ok {
			merged.Values[f] = copyStringPtr(v)
		} else {
			merged.Values[f] = nil
		}
	}

	// 第三遍：分类差异字段——哪些可以自动解决，哪些需要人工干预
	var manualDiffFields []string // 两者都有值且不同，需人工决定
	autoResolvedCount := 0

	for _, f := range diffFields {
		valA := rowA.Values[f]
		valB, bHas := rowB.Values[f]
		if !bHas {
			continue
		}

		aIsEmpty := isNullOrEmpty(valA)
		bIsEmpty := isNullOrEmpty(valB)

		if aIsEmpty && !bIsEmpty {
			// A为空/NULL，B有值 => 自动用B的值
			merged.Values[f] = copyStringPtr(valB)
			m.stats.NullAutoFilled++
			autoResolvedCount++
			fmt.Printf("  [自动填充] 字段[%s]: A为空/NULL, 自动使用B的值: %s\n", f, displayValue(valB))
		} else if !aIsEmpty && bIsEmpty {
			// A有值，B为空/NULL => 自动保留A的值
			autoResolvedCount++
			fmt.Printf("  [自动保留] 字段[%s]: B为空/NULL, 自动保留A的值: %s\n", f, displayValue(valA))
		} else {
			// 两者都有值且不同 => 需要根据策略决定
			manualDiffFields = append(manualDiffFields, f)
		}
	}

	// 如果所有差异都已自动解决，无需人工干预
	if len(manualDiffFields) == 0 {
		fmt.Printf("  [结果] 所有差异已自动解决（共 %d 个自动处理）\n", autoResolvedCount)
		diffStr := strings.Join(diffFields, ",")
		return m.buildCRowMerged(merged, "MERGE_A", true, diffStr)
	}

	// 存在需要人工决定的差异字段
	fmt.Printf("\n[待决] 以下 %d 个字段两者都有值但不同，需根据策略决定:\n\n", len(manualDiffFields))
	for _, f := range manualDiffFields {
		fmt.Printf("    字段[%s]: A=%-30s B=%s\n", f, displayValue(rowA.Values[f]), displayValue(rowB.Values[f]))
	}

	// 根据策略决定
	var choice ConflictStrategy
	switch m.config.Strategy {
	case UseA:
		choice = UseA
		fmt.Printf("\n    [策略] 配置为自动以A表数据为准\n")
	case UseB:
		choice = UseB
		fmt.Printf("\n    [策略] 配置为自动以B表数据为准\n")
	case AskUser:
		// 交互式询问用户
		choice = m.askUserChoice(manualDiffFields, rowA, rowB)
	}

	diffStr := strings.Join(diffFields, ",")

	if choice == UseA {
		m.stats.ConflictUseA++
		fmt.Printf("    [结果] 以A表数据写入C表\n")
		return m.buildCRowMerged(merged, "MERGE_A", true, diffStr)
	}

	// 以B为准：用B的值覆盖冲突字段
	m.stats.ConflictUseB++
	for _, f := range manualDiffFields {
		if valB, ok := rowB.Values[f]; ok {
			merged.Values[f] = copyStringPtr(valB)
		}
	}
	fmt.Printf("  [结果] 以B表数据写入C表\n")
	return m.buildCRowMerged(merged, "MERGE_B", true, diffStr)
}

// askUserChoice 交互式询问用户选择，等待用户输入后才继续
func (m *Merger) askUserChoice(diffFields []string, rowA, rowB *rowData) ConflictStrategy {
	fmt.Println()
	fmt.Println("  ┌────────────────────────────────────────────┐")
	fmt.Println("  │请选择以哪个表的数据为准                    │")
	fmt.Println("  │                                            │")
	fmt.Println("  │  输入 A : 使用 A 表的值                    │")
	fmt.Println("  │  输入 B : 使用 B 表的值                    │")
	fmt.Println("  └────────────────────────────────────────────┘")

	for {
		fmt.Printf("  >>> 请输入您的选择 (A/B): ")

		// 使用全局的 stdinReader 读取，确保不会因多次创建丢失缓冲区
		input, err := m.stdinReader.ReadString('\n')
		if err != nil {
			logx.Errorf("读取用户输入失败: %v", err)
			fmt.Printf("  [错误] 读取输入失败: %v，默认使用A表数据\n", err)
			return UseA
		}

		input = strings.TrimSpace(input)
		input = strings.TrimRight(input, "\r\n")
		input = strings.ToUpper(strings.TrimSpace(input))

		switch input {
		case "A":
			fmt.Printf("  [用户选择] ✓ 以A表数据为准\n")
			return UseA
		case "B":
			fmt.Printf("  [用户选择] ✓ 以B表数据为准\n")
			return UseB
		default:
			fmt.Printf("  [提示] 无效输入 \"%s\"，请输入 A 或 B\n", input)
		}
	}
}

// buildCRowFromAWithMeta 从A表数据构建C表行，带元数据
func (m *Merger) buildCRowFromAWithMeta(rowA *rowData, source string, conflict bool, diffFields string) *rowData {
	result := &rowData{Values: make(map[string]*string)}
	for _, f := range m.fieldNamesC {
		if v, ok := rowA.Values[f]; ok {
			result.Values[f] = copyStringPtr(v)
		} else {
			result.Values[f] = nil
		}
	}
	result.Values["_source"] = strPtr(source)
	if conflict {
		result.Values["_conflict"] = strPtr("1")
	} else {
		result.Values["_conflict"] = strPtr("0")
	}
	if diffFields != "" {
		result.Values["_diff_fields"] = strPtr(diffFields)
	} else {
		result.Values["_diff_fields"] = nil
	}
	return result
}

// buildCRowFromB 从B表数据构建C表行
func (m *Merger) buildCRowFromB(rowB *rowData) *rowData {
	result := &rowData{Values: make(map[string]*string)}
	for _, f := range m.fieldNamesC {
		// B表中忽略的字段不写入
		if m.ignoreSetB[f] {
			result.Values[f] = nil
			continue
		}
		if v, ok := rowB.Values[f]; ok && m.bFieldInC[f] {
			result.Values[f] = copyStringPtr(v)
		} else {
			result.Values[f] = nil
		}
	}
	result.Values["_source"] = strPtr("B")
	result.Values["_conflict"] = strPtr("0")
	result.Values["_diff_fields"] = nil
	return result
}

// buildCRowMerged 从合并数据构建C表行
func (m *Merger) buildCRowMerged(merged *rowData, source string, conflict bool, diffFields string) *rowData {
	result := &rowData{Values: make(map[string]*string)}
	for _, f := range m.fieldNamesC {
		if v, ok := merged.Values[f]; ok {
			result.Values[f] = copyStringPtr(v)
		} else {
			result.Values[f] = nil
		}
	}
	result.Values["_source"] = strPtr(source)
	if conflict {
		result.Values["_conflict"] = strPtr("1")
	} else {
		result.Values["_conflict"] = strPtr("0")
	}
	if diffFields != "" {
		result.Values["_diff_fields"] = strPtr(diffFields)
	} else {
		result.Values["_diff_fields"] = nil
	}
	return result
}

// batchInsertC 批量插入数据到C表
func (m *Merger) batchInsertC(rows []rowData) error {
	if len(rows) == 0 {
		fmt.Printf("[信息] 没有数据需要写入\n")
		return nil
	}

	// C表的所有字段（包括元数据字段）
	allFields := make([]string, 0, len(m.fieldNamesC)+3)
	allFields = append(allFields, m.fieldNamesC...)
	allFields = append(allFields, "_source", "_conflict", "_diff_fields")

	quotedFields := make([]string, len(allFields))
	for i, f := range allFields {
		quotedFields[i] = fmt.Sprintf("`%s`", f)
	}
	fieldStr := strings.Join(quotedFields, ", ")

	placeholders := make([]string, len(allFields))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	singleRow := "(" + strings.Join(placeholders, ", ") + ")"

	batchSize := m.config.BatchSize
	total := len(rows)
	inserted := 0

	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}
		batch := rows[i:end]

		rowPlaceholders := make([]string, len(batch))
		args := make([]interface{}, 0, len(batch)*len(allFields))

		for j, row := range batch {
			rowPlaceholders[j] = singleRow
			for _, f := range allFields {
				val := row.Values[f]
				if val == nil {
					args = append(args, nil)
				} else {
					args = append(args, *val)
				}
			}
		}

		insertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
			m.config.TableC, fieldStr, strings.Join(rowPlaceholders, ", "))

		if _, err := m.db.Exec(insertSQL, args...); err != nil {
			logx.Errorf("批量插入C表失败(行 %d-%d): %v", i+1, end, err)
			return fmt.Errorf("批量插入C表失败: %v", err)
		}
		inserted += len(batch)
		fmt.Printf("\r[写入] 已写入 %d/%d 条记录", inserted, total)
	}
	fmt.Println()
	return nil
}

// ==================== 工具函数 ====================

// valuesEqual 比较两个值是否相等，正确处理 NULL
func valuesEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// isNullOrEmpty 判断值是否为 NULL 或空字符串
func isNullOrEmpty(v *string) bool {
	if v == nil {
		return true
	}
	return *v == ""
}

// copyStringPtr 复制字符串指针
func copyStringPtr(v *string) *string {
	if v == nil {
		return nil
	}
	s := *v
	return &s
}

// strPtr 返回字符串的指针
func strPtr(s string) *string {
	return &s
}

// displayValue 格式化显示值（处理NULL和空字符串）
func displayValue(v *string) string {
	if v == nil {
		return "<NULL>"
	}
	if *v == "" {
		return "<空字符串>"
	}
	return *v
}
