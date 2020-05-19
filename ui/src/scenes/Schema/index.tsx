import React, { useCallback, useState } from "react"
import styled from "styled-components"

import data from "mocks/schema.json"
import { theme } from "theme"
import type { TableShape } from "types"
import { color } from "utils"
import Header from "./Header"
import Table from "./Table"

const schema = data as TableShape[]

const Columns = styled.div`
  display: flex;
  flex-direction: column;
  flex: 1;
  background: ${color("draculaBackground")};
  font-family: ${theme.fontMonospace};
  overflow: auto;
`

const Schema = () => {
  const [opened, setOpened] = useState<string>()
  const handleChange = useCallback((name: string) => {
    setOpened(name)
  }, [])

  return (
    <>
      <Header />
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
