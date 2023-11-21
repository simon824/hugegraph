graph.schema().propertyKey('totalVisitTime').asLong().ifNotExist().create()
graph.schema().propertyKey('leaveTime').asLong().ifNotExist().create()
graph.schema().propertyKey('grid').asText().ifNotExist().create()
graph.schema().propertyKey('COVID-19').asInt().ifNotExist().create()
graph.schema().propertyKey('arriveTime').asLong().ifNotExist().create()
graph.schema().propertyKey('province').asText().ifNotExist().create()
graph.schema().propertyKey('phone').asText().ifNotExist().create()
graph.schema().vertexLabel('phone').properties('phone', 'COVID-19').primaryKeys('phone').nullableKeys('COVID-19').enableLabelIndex(true).ifNotExist().create()
graph.schema().vertexLabel('grid').properties('grid').primaryKeys('grid').enableLabelIndex(true).ifNotExist().create()
graph.schema().edgeLabel('visit').sourceLabel('phone').targetLabel('grid').properties('province', 'arriveTime', 'leaveTime', 'totalVisitTime').multiTimes().sortKeys('arriveTime').enableLabelIndex(true).ifNotExist().create()

def findCloseTouch(source) {
    edges = g.V(source).outE()
    Set<Vertex> grids = new HashSet<>()
    Set<Vertex> mijies = new HashSet<>()
    List<Edge> edges1 = new ArrayList<>()

    while (edges.hasNext()) {
        accessed = false
        edge = edges.next()
        grid = edge.inVertex()
        if (grids.contains(grid)) {
            accessed = true
        } else {
            grids.add(grid)
        }
        dangerGrid = false
        if (edge.value('totalVisitTime') >= 108000000) {
            dangerGrid = true
        }
        arrive1 = edge.value('arriveTime')
        leave1 = edge.value('leaveTime')
        if (!accessed) {
            edges1 = g.V(grid).inE().toList()
        }
        for (Edge edge1 : edges1) {
            person = edge1.outVertex()
            if (person.id().asObject().equals(source)) {
                continue
            }
            if (mijies.contains(person)) {
                continue
            }
            arrive2 = edge1.value('arriveTime')
            leave2 = edge1.value('leaveTime')
            if (leave1 >= arrive2 && leave2 >= arrive1) {
                low = Math.max(arrive1, arrive2)
                high = Math.min(leave1, leave2)
                if (high - low < 600000) {
                    continue
                }
                if (dangerGrid || edge1.value('totalVisitTime') >= 108000000) {
                    mijies.add(person)
                }
            }
        }
    }
    return mijies
}


graph.schema().propertyKey('phone').asLong().ifNotExist().create()
graph.schema().vertexLabel('user').useCustomizeNumberId().enableLabelIndex(false).ifNotExist().create()
graph.schema().edgeLabel('relation').sourceLabel('user').targetLabel('user').enableLabelIndex(false).ifNotExist().create()

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

//==================================================================================================================================================


//GET /graphspaces/${graphspace}/graphs/${graph}/traversers/kout?source="xxxx"&direction=BOTH&max_depth=4&algorithm="deep_first"&limit=100


//==================================================================================================================================================

graph.schema().propertyKey('new_card').asInt().ifNotExist().create()
graph.schema().propertyKey('userid').asText().ifNotExist().create()
graph.schema().propertyKey('province').asInt().ifNotExist().create()
graph.schema().propertyKey('id_card').asText().ifNotExist().create()
graph.schema().propertyKey('phone').asText().ifNotExist().create()
graph.schema().vertexLabel('user').properties('userid', 'new_card', 'province').useCustomizeStringId().enableLabelIndex(false).ifNotExist().create()
graph.schema().vertexLabel('id_card').useCustomizeStringId().enableLabelIndex(false).ifNotExist().create()
graph.schema().edgeLabel('own').sourceLabel('user').targetLabel('id_card').enableLabelIndex(true).ifNotExist().create()
graph.schema().edgeLabel('call').sourceLabel('user').targetLabel('user').enableLabelIndex(true).ifNotExist().create()

def find1StepNeighbors() {
    sources = g.V().has('province', 10000).has('new_card', 1).toList()
    List<Map<String, Object>> results = new LinkedList<>()
    for (Vertex v : sources) {
        Map<String, Object> result = new HashMap<>()
        result.put('province', 10000)
        result.put('user', v.id())
        result.put('neighbors', g.V(v).both('call').dedup().count().next())
        results.add(result)
    }
    return results
}


def findSelfCall() {
    sources = g.V().has('province', 10000).has('new_card', 1).toList()
    List<Map<String, Object>> results = new LinkedList<>()

    for (Vertex v : sources) {
        flag = 0
        Map<String, Object> result = new HashMap<>()
        result.put('province', 10000)
        result.put('user', v.id())

        user = g.V(v).out('own').toList()
        neighbors = g.V(v).both('call').toSet()
        for (Vertex v1 : neighbors) {
            user1 = g.V(v1).out('own').toList()
            if (user.equals(user1)) {
                flag = 1
                break
            }
        }
        result.put('self-call', flag)
        results.add(result)
    }
    return results
}


def find1StepNeighborsCall() {
    sources = g.V().has('province', 10000).has('new_card', 1).toList()
    List<Map<String, Object>> results = new LinkedList<>()

    for (Vertex v : sources) {
        Map<String, Object> result = new HashMap<>()
        result.put('province', 10000)
        result.put('user', v.id())
        neighbors1 = g.V(v).both('call').toSet()
        neighbors2 = g.V(neighbors1).both('call').toSet()
        neighbors1.retainAll(neighbors2)
        result.put('neighbors', neighbors1.size())
        results.add(result)
    }
    return results
}


def findSelfNumberSameNeighbors() {
    sources = g.V().has('province', 10000).has('new_card', 1).toList()
    List<Map<String, Object>> results = new LinkedList<>()

    for (Vertex v : sources) {
        Map<String, Object> result = new HashMap<>()
        result.put('province', 10000)
        result.put('user', v.id())
        others = g.V(v).out('own').as('x').in('own').where(neq('x')).toList()
        neighbors1 = g.V(v).both('call').toSet()
        neighbors2 = g.V(others).both('call').toSet()
        neighbors1.retainAll(neighbors2)
        result.put('same-neighbors', neighbors1.size())
        results.add(result)
    }
    return results
}


def findSelf1StepAndOther2StepNeighbors() {
    sources = g.V().has('province', 10000).has('new_card', 1).toList()
    List<Map<String, Object>> results = new LinkedList<>()

    for (Vertex v : sources) {
        Map<String, Object> result = new HashMap<>()
        result.put('province', 10000)
        result.put('user', v.id())
        others = g.V(v).out('own').as('x').in('own').where(neq('x')).toList()

        neighbors1 = g.V(v).both('call').toSet()
        neighbors2 = g.V(others).both('call').both('call').toSet()
        neighbors1.retainAll(neighbors2)
        result.put('same-neighbors', neighbors1.size())
        results.add(result)
    }
    return results
}

