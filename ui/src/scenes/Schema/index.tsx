import React, { useCallback, useState } from "react"
import styled from "styled-components"
import { Database } from "@styled-icons/entypo"

import { Pane, PaneTitle, Text } from "components"
import data from "mocks/schema.json"
import { theme } from "theme"
import type { TableShape } from "types"

import Table from "./Table"

const schema = data as TableShape[]

const Columns = styled(Pane)`
  font-family: ${theme.fontMonospace};
`

const DatabaseIcon = styled(Database)`
  margin-right: 1rem;
`

const Schema = () => {
  const [opened, setOpened] = useState<string>()
  const handleChange = useCallback((name: string) => {
    setOpened(name)
  }, [])

  return (
    <>
      <PaneTitle>
        <Text color="draculaForeground">
          <DatabaseIcon size="18px" />
          Tables
        </Text>
      </PaneTitle>
      <Columns>
        {schema.map((table) => (
          <Table
            {...table}
            expanded={table.name === opened}
            key={table.name}
            onChange={handleChange}
          />
        ))}
      </Columns>
    </>
  )
}

export default Schema
