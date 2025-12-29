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

package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

// Route 路由定义
type Route struct {
	Method      string
	Path        string
	Handler     http.HandlerFunc
	Middlewares []Middleware
}

// Router 路由管理器
type Router struct {
	muxRouter *mux.Router
	routes    []Route
}

// NewRouter 创建新的路由管理器
func NewRouter() *Router {
	return &Router{
		muxRouter: mux.NewRouter(),
		routes:    make([]Route, 0),
	}
}

// AddRoute 添加路由
func (r *Router) AddRoute(method, path string, handler http.HandlerFunc, middlewares ...Middleware) {
	route := Route{
		Method:      method,
		Path:        path,
		Handler:     handler,
		Middlewares: middlewares,
	}

	r.routes = append(r.routes, route)
}

// AddRoutes 批量添加路由
func (r *Router) AddRoutes(routes []Route) {
	r.routes = append(r.routes, routes...)
}

// Build 构建路由器
func (r *Router) Build() *mux.Router {
	// 每次构建时重置底层mux路由器，避免重复注册同一路由
	r.muxRouter = mux.NewRouter().StrictSlash(true)

	// 按照相反的顺序注册路由，这样后面的路由会覆盖前面的路由
	// 这确保了全局路由（如 /_search）不会被索引路由（如 /{index}）覆盖
	for i := len(r.routes) - 1; i >= 0; i-- {
		route := r.routes[i]
		handler := route.Handler

		// 应用中间件
		if len(route.Middlewares) > 0 {
			for j := len(route.Middlewares) - 1; j >= 0; j-- {
				handler = route.Middlewares[j](handler)
			}
		}

		r.muxRouter.HandleFunc(route.Path, handler).Methods(route.Method)
	}

	return r.muxRouter
}

// 注意：此包是ES协议专用的HTTP基础设施
// Redis和MySQL协议使用TCP，不需要HTTP基础设施
