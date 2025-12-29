// Copyright (c) 2024 TigerDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dsl

import (
	"fmt"

	"github.com/lscgzwd/tiggerdb/geo"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// ========== 地理查询类型 ==========

// parseGeoBoundingBox 解析geo_bounding_box查询
func (p *QueryParser) parseGeoBoundingBox(body interface{}) (query.Query, error) {
	geoMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("geo_bounding_box query must be an object")
	}

	var field string
	var topLeft, bottomRight []float64

	for fieldName, fieldValue := range geoMap {
		field = fieldName
		coordsMap, ok := fieldValue.(map[string]interface{})
		if !ok {
			continue
		}

		if topLeftVal, ok := coordsMap["top_left"]; ok {
			topLeftArr, ok := topLeftVal.([]interface{})
			if ok && len(topLeftArr) >= 2 {
				lon, _ := p.toFloat64(topLeftArr[0])
				lat, _ := p.toFloat64(topLeftArr[1])
				topLeft = []float64{lon, lat}
			}
		}

		if bottomRightVal, ok := coordsMap["bottom_right"]; ok {
			bottomRightArr, ok := bottomRightVal.([]interface{})
			if ok && len(bottomRightArr) >= 2 {
				lon, _ := p.toFloat64(bottomRightArr[0])
				lat, _ := p.toFloat64(bottomRightArr[1])
				bottomRight = []float64{lon, lat}
			}
		}

		break
	}

	if len(topLeft) < 2 || len(bottomRight) < 2 {
		return nil, fmt.Errorf("geo_bounding_box query must have top_left and bottom_right coordinates")
	}

	geoQuery := query.NewGeoBoundingBoxQuery(topLeft[0], topLeft[1], bottomRight[0], bottomRight[1])
	geoQuery.SetField(field)

	if boost, ok := geoMap["boost"].(float64); ok {
		geoQuery.SetBoost(boost)
	}

	return geoQuery, nil
}

// parseGeoDistance 解析geo_distance查询
func (p *QueryParser) parseGeoDistance(body interface{}) (query.Query, error) {
	geoMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("geo_distance query must be an object")
	}

	var field string
	var lon, lat float64
	var distance string

	for fieldName, fieldValue := range geoMap {
		if fieldName == "distance" {
			if distStr, ok := fieldValue.(string); ok {
				distance = distStr
			}
			continue
		}
		if fieldName == "boost" {
			continue
		}

		field = fieldName
		coordsMap, ok := fieldValue.(map[string]interface{})
		if !ok {
			if coordsArr, ok := fieldValue.([]interface{}); ok && len(coordsArr) >= 2 {
				lon, _ = p.toFloat64(coordsArr[0])
				lat, _ = p.toFloat64(coordsArr[1])
			}
			continue
		}

		if lonVal, ok := coordsMap["lon"]; ok {
			lon, _ = p.toFloat64(lonVal)
		}
		if latVal, ok := coordsMap["lat"]; ok {
			lat, _ = p.toFloat64(latVal)
		}
	}

	if distance == "" {
		return nil, fmt.Errorf("geo_distance query must have 'distance' field")
	}

	geoQuery := query.NewGeoDistanceQuery(lon, lat, distance)
	geoQuery.SetField(field)

	if boost, ok := geoMap["boost"].(float64); ok {
		geoQuery.SetBoost(boost)
	}

	return geoQuery, nil
}

// parseGeoPolygon 解析geo_polygon查询
func (p *QueryParser) parseGeoPolygon(body interface{}) (query.Query, error) {
	geoMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("geo_polygon query must be an object")
	}

	var field string
	var points []interface{}

	for fieldName, fieldValue := range geoMap {
		if fieldName == "boost" {
			continue
		}

		field = fieldName
		coordsMap, ok := fieldValue.(map[string]interface{})
		if !ok {
			continue
		}

		if pointsVal, ok := coordsMap["points"]; ok {
			if pointsArr, ok := pointsVal.([]interface{}); ok {
				points = pointsArr
			}
		}
		break
	}

	if len(points) < 3 {
		return nil, fmt.Errorf("geo_polygon query must have at least 3 points")
	}

	geoPoints := make([]geo.Point, 0, len(points))
	for _, point := range points {
		if pointArr, ok := point.([]interface{}); ok && len(pointArr) >= 2 {
			lon, _ := p.toFloat64(pointArr[0])
			lat, _ := p.toFloat64(pointArr[1])
			geoPoints = append(geoPoints, geo.Point{Lon: lon, Lat: lat})
		}
	}

	if len(geoPoints) < 3 {
		return nil, fmt.Errorf("geo_polygon query must have at least 3 valid points")
	}

	geoQuery := query.NewGeoBoundingPolygonQuery(geoPoints)
	geoQuery.SetField(field)

	if boost, ok := geoMap["boost"].(float64); ok {
		geoQuery.SetBoost(boost)
	}

	return geoQuery, nil
}

// parseGeoShape 解析geo_shape查询
func (p *QueryParser) parseGeoShape(body interface{}) (query.Query, error) {
	geoMap, ok := body.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("geo_shape query must be an object")
	}

	var field string
	var shapeMap map[string]interface{}
	var relation string = "intersects"

	for fieldName, fieldValue := range geoMap {
		if fieldName == "boost" {
			continue
		}

		field = fieldName
		coordsMap, ok := fieldValue.(map[string]interface{})
		if !ok {
			continue
		}

		if shapeVal, ok := coordsMap["shape"]; ok {
			if shape, ok := shapeVal.(map[string]interface{}); ok {
				shapeMap = shape
			}
		}

		if relVal, ok := coordsMap["relation"].(string); ok {
			relation = relVal
		}
		break
	}

	if shapeMap == nil {
		return nil, fmt.Errorf("geo_shape query must have 'shape' field")
	}

	shapeType, ok := shapeMap["type"].(string)
	if !ok {
		return nil, fmt.Errorf("geo_shape query must have 'type' field in shape")
	}

	var geoQuery *query.GeoShapeQuery

	switch shapeType {
	case "circle":
		coordinates := shapeMap["coordinates"]
		if coordsArr, ok := coordinates.([]interface{}); ok && len(coordsArr) >= 2 {
			lon, err := p.toFloat64(coordsArr[0])
			if err != nil {
				return nil, fmt.Errorf("invalid longitude in geo_shape circle: %w", err)
			}
			lat, err := p.toFloat64(coordsArr[1])
			if err != nil {
				return nil, fmt.Errorf("invalid latitude in geo_shape circle: %w", err)
			}
			radius := "1km"
			if props, ok := shapeMap["properties"].(map[string]interface{}); ok {
				if radiusVal, ok := props["radius"].(string); ok {
					radius = radiusVal
				}
			}
			geoQuery, err = query.NewGeoShapeCircleQuery([]float64{lon, lat}, radius, relation)
			if err != nil {
				return nil, fmt.Errorf("failed to create geo_shape circle query: %w", err)
			}
		} else {
			return nil, fmt.Errorf("geo_shape circle query must have valid coordinates array")
		}
	default:
		return nil, fmt.Errorf("geo_shape query type '%s' is not fully supported yet", shapeType)
	}

	if geoQuery == nil {
		return nil, fmt.Errorf("failed to create geo_shape query for type '%s'", shapeType)
	}

	geoQuery.SetField(field)

	if boost, ok := geoMap["boost"].(float64); ok {
		geoQuery.SetBoost(boost)
	}

	return geoQuery, nil
}
