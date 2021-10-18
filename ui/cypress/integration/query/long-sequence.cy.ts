import * as cypress from "cypress"

it("runs a long_sequence query with 2000 rows", function () {
  cy.getSqlEditor().type("SELECT x FROM long_sequence(2000)")
  cy.getByDataTest("button-run-query").click()
  cy.getGridRow()
    .first()
    .within(() => {
      cy.getGridColumn().should("have.text", 1)
    })
  cy.getGridViewport().scrollTo("bottom", { duration: 5000 })
  cy.getGridRow(true).within(() => {
    cy.getGridColumn().should("have.text", 2000)
  })
})
