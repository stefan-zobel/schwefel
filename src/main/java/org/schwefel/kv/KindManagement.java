/*
 * Copyright 2021, 2023 Stefan Zobel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schwefel.kv;

import java.util.Set;

public interface KindManagement {

    Set<Kind> getKinds();
    Kind getKind(String kindName);
    Kind getOrCreateKind(String kindName);
    Kind getDefaultKind();
    void compact(String kindName);
    void compactAll();
    void deleteKind(Kind kind);
}
