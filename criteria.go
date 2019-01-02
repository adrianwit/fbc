package fbc

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
)

//asCriteriaMap returns criteria  map
func asCriteriaMap(sqlCriteria *dsc.SQLCriteria, paramIterator toolbox.Iterator) (map[string]interface{}, error) {
	var result = make(map[string]interface{})
	if len(sqlCriteria.Criteria) == 0 {
		return result, nil
	}
	var value interface{}

	if len(sqlCriteria.Criteria) == 1 {
		criteria := sqlCriteria.Criteria[0]
		if !(criteria.Operator == "=" || criteria.Operator == "IN") {
			return nil, fmt.Errorf("unsuppored operator: %v", criteria.Operator)
		}
		column, ok := criteria.LeftOperand.(string)
		columnValue := criteria.RightOperand.(string)
		if !ok || column == "?" {
			column, ok = criteria.RightOperand.(string)
		}
		bindParamCount := strings.Count(columnValue, "?")
		switch bindParamCount {
		case 0:
			result[column] = strings.Trim(columnValue, "'")
		case 1:
			if !paramIterator.HasNext() {
				return nil, fmt.Errorf("missing bind param: %v %v %v", criteria.LeftOperand, criteria.Operator, criteria.RightOperand)
			}
			if err := paramIterator.Next(&value); err != nil {
				return nil, err
			}
			result[column] = value
			break
		default:
			var values = make([]interface{}, 0)
			for i := 0; i < bindParamCount; i++ {
				if !paramIterator.HasNext() {
					return nil, fmt.Errorf("missing bind param: %v %v %v", criteria.LeftOperand, criteria.Operator, criteria.RightOperand)
				}

				if err := paramIterator.Next(&value); err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			result[column] = values
		}

	} else {
		return nil, fmt.Errorf("criteria on one key is supported")
	}
	return result, nil
}
