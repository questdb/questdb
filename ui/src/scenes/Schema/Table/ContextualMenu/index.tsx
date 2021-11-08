/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

import React, { useCallback, useContext } from "react"
import { ContextMenu, MenuItem } from "components/ContextMenu"
import { QuestContext } from "providers"
import * as QuestDB from "utils/questdb"
import { formatTableSchemaQueryResult } from "./services"

type Props = {
  name: string
  partitionBy: string
}

const ContextualMenu = ({ name, partitionBy }: Props) => {
  const { quest } = useContext(QuestContext)

  const handleCopySchemaToClipboard = useCallback(() => {
    void quest.queryRaw(`table_columns('${name}')`).then((result) => {
      if (result.type === QuestDB.Type.DQL && result.count > 0) {
        const formattedResult = formatTableSchemaQueryResult(
          name,
          partitionBy,
          result,
        )
        void navigator.clipboard.writeText(formattedResult)
      }
    })
  }, [quest, name, partitionBy])

  return (
    <ContextMenu id={name}>
      <MenuItem onClick={handleCopySchemaToClipboard}>
        Copy Schema To Clipboard
      </MenuItem>
      <MenuItem divider />
      <MenuItem>Close</MenuItem>
    </ContextMenu>
  )
}

export default ContextualMenu
