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

package org.apache.hugegraph.auth;

import java.util.List;

import javax.security.sasl.AuthenticationException;

import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.meta.MetaManager;

public interface AuthManager {

    public boolean close();

    public Id createUser(HugeUser user, boolean required);

    public HugeUser updateUser(HugeUser user, boolean required);

    public HugeUser deleteUser(Id id, boolean required);

    public HugeUser findUser(String name, boolean required);

    public HugeUser getUser(Id id, boolean required);

    public List<HugeUser> listUsers(List<Id> ids, boolean required);

    public List<HugeUser> listUsersByGroup(String group,
                                           long limit, boolean required);

    public List<HugeUser> listAllUsers(long limit, boolean required);

    public Id createGroup(HugeGroup group, boolean required);

    public HugeGroup updateGroup(HugeGroup group, boolean required);

    public HugeGroup deleteGroup(Id id, boolean required);

    public HugeGroup findGroup(String name, boolean required);

    public List<HugeGroup> listGroups(long limit, boolean required);

    public List<HugeGroup> listGroupsByUser(String user, long limit,
                                            boolean required);

    public Id createSpaceManager(String graphSpace, String owner);

    public void deleteSpaceManager(String graphSpace, String owner);

    public List<String> listSpaceManager(String graphSpace);

    public boolean isSpaceManager(String owner);

    public boolean isSpaceManager(String graphSpace, String owner);

    public Id createAdminManager(String user);

    public void deleteAdminManager(String user);

    public List<String> listAdminManager();

    public boolean isAdminManager(String user);

    public Id createDefaultRole(String graphSpace, String owner,
                                HugeDefaultRole role, String graph);

    public Id createSpaceDefaultRole(String graphSpace, String owner,
                                     HugeDefaultRole role);

    public void initSpaceDefaultRoles(String graphSpace);

    public void createGraphDefaultRole(String graphSpace, String graph);

    public String getGraphDefaultRole(String graph, String role);

    public void deleteGraphDefaultRole(String graphSpace, String graph);

    public void deleteDefaultRole(String graphSpace, String owner,
                                  HugeDefaultRole role);

    public void deleteDefaultRole(String graphSpace, String owner,
                                  HugeDefaultRole role, String graph);

    public boolean isDefaultRole(String owner, HugeDefaultRole role);

    public boolean isDefaultRole(String graphSpace, String owner,
                                 HugeDefaultRole role);

    public boolean isDefaultRole(String graphSpace, String graph, String owner,
                                 HugeDefaultRole role);

    public Id createRole(String graphSpace, HugeRole role,
                         boolean required);

    public HugeRole updateRole(String graphSpace, HugeRole role,
                               boolean required);

    public HugeRole deleteRole(String graphSpace, Id id, boolean required);

    public HugeRole getRole(String graphSpace, Id id, boolean required);

    public List<HugeRole> listRoles(String graphSpace, List<Id> ids,
                                    boolean required);

    public List<HugeRole> listAllRoles(String graphSpace, long limit,
                                       boolean required);

    public Id setDefaultSpace(String graphSpace, String user);

    public String getDefaultSpace(String user);

    public void unsetDefaultSpace(String graphSpace, String user);

    public Id setDefaultGraph(String graphSpace, String graph,
                              String user);

    public String getDefaultGraph(String graphSpace, String user);

    public void unsetDefaultGraph(String graphSpace, String graph,
                                  String user);

    public Id createTarget(String graphSpace, HugeTarget target,
                           boolean required);

    public HugeTarget updateTarget(String graphSpace, HugeTarget target,
                                   boolean required);

    public HugeTarget deleteTarget(String graphSpace, Id id, boolean required);

    public HugeTarget getTarget(String graphSpace, Id id, boolean required);

    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids,
                                        boolean required);

    public List<HugeTarget> listAllTargets(String graphSpace, long limit,
                                           boolean required);

    public Id createBelong(String graphSpace, HugeBelong belong,
                           boolean required);

    public HugeBelong updateBelong(String graphSpace, HugeBelong belong,
                                   boolean required);

    public HugeBelong deleteBelong(String graphSpace, Id id, boolean required);

    public HugeBelong getBelong(String graphSpace, Id id, boolean required);

    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids,
                                       boolean required);

    public List<HugeBelong> listAllBelong(String graphSpace, long limit,
                                          boolean required);

    public List<HugeBelong> listBelongBySource(String graphSpace, Id user,
                                               String link, long limit,
                                               boolean required);

    public List<HugeBelong> listBelongByTarget(String graphSpace, Id target,
                                               String link, long limit,
                                               boolean required);

    public Id createAccess(String graphSpace, HugeAccess access,
                           boolean required);

    public HugeAccess updateAccess(String graphSpace, HugeAccess access,
                                   boolean required);

    public HugeAccess deleteAccess(String graphSpace, Id id, boolean required);

    public HugeAccess getAccess(String graphSpace, Id id, boolean required);

    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids,
                                       boolean required);

    public List<HugeAccess> listAllAccess(String graphSpace, long limit,
                                          boolean required);

    public List<HugeAccess> listAccessByRole(String graphSpace, Id role,
                                             long limit, boolean required);

    public List<HugeAccess> listAccessByTarget(String graphSpace, Id target,
                                               long limit, boolean required);

    public void copyPermissionsToOther(String graphSpace, String originNickname,
                                       String graphNickname, boolean auth);

    public void copyPermissionsToOther(String originSpace, String origin,
                                       String otherSpace, String nickname,
                                       boolean auth);

    public void updateNicknamePermission(String graphSpace, String origin,
                                         String nickname, boolean auth);

    public List<String> listWhiteIp();

    public void setWhiteIpList(List<String> whiteIpList);

    public boolean getWhiteIpStatus();

    public void setWhiteIpStatus(boolean status);

    public List<String> listGraphSpace();

    public HugeUser matchUser(String name, String password);

    public RolePermission rolePermission(AuthElement element);

    public String loginUser(String username, String password,
                            long expire)
            throws AuthenticationException;

    public void logoutUser(String token);

    public String createToken(String username);

    public Id createKgUser(HugeUser user);

    public String createToken(String username, long expire);

    public void processEvent(MetaManager.AuthEvent event);

    public UserWithRole validateUser(String username, String password);

    public UserWithRole validateUser(String token);

    public String username();
}
