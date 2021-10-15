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

it("runs a select query from telemetry_config", function () {
  cy.getByCustomData("table-name", "telemetry_config").within(() => {
    cy.getByDataTest("add-to-editor").click()
  })
  cy.getByDataTest("button-run-query").click()
  cy.getByCustomData("grid-row-num", "1").should("not.be.empty")
  cy.getGridHeaderRow().within(() => {
    cy.fixture("telemetryConfigColumns").then((columns: string[]) => {
      columns.forEach((column) => {
        cy.get(".qg-header").contains(column)
      })
    })
    cy.execQuery("telemetry_config").then(
      (response: Cypress.Response<{ columns: [] }>) => {
        const { columns } = response.body
        columns.forEach((column: { name: string }) => {
          cy.get(".qg-header").contains(column.name)
        })
      },
    )
  })
  cy.getByDataTest("notifications-wrapper").within(() => {
    cy.getByCustomData("notification-type", "success").should("exist")
  })
})
