/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import * as cypress from "cypress"

it("should render schema", function () {
  cy.getByDataTest("schema-wrapper").should("exist")
  cy.execQuery("tables").then(
    (response: Cypress.Response<{ count: number }>) => {
      const { count } = response.body
      cy.getByDataTest("table-wrapper").should("have.length", count)
    },
  )
})

it("should add a table name to editor", function () {
  cy.getByDataTest("schema-wrapper").within(() => {
    cy.getByCustomData("table-name", "telemetry_config").within(() => {
      cy.getByDataTest("add-to-editor").click()
    })
  })
  cy.getSqlEditorValue().should("contain", "'telemetry_config'")
})

it("should expand table columns", function () {
  cy.getByCustomData("table-name", "telemetry_config")
    .click()
    .within(() => {
      cy.getByDataTest("table-columns")
        .should("be.visible")
        .within(() => {
          cy.execQuery("telemetry_config").then(
            (response: Cypress.Response<{ columns: [] }>) => {
              const { columns } = response.body
              cy.getByDataTest("row-name").should("have.length", columns.length)
            },
          )
        })
    })
})

it("should add a column name to editor", function () {
  cy.getByCustomData("table-name", "telemetry_config")
    .click()
    .within(() => {
      cy.getByCustomData("row-name", "id").within(() => {
        cy.getByDataTest("add-to-editor").click()
      })
    })
  cy.getSqlEditorValue().should("contain", "id")
})
