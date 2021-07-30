'use strict'

class Result {
  constructor (types, arrayMode) {
    this._types = types
    this._arrayMode = arrayMode

    this.command = undefined
    this.rowCount = undefined
    this.fields = []
    this.rows = []
  }

  consumeCommand (pq) {
    this.command = pq.cmdStatus().split(' ')[0]
    this.rowCount = parseInt(pq.cmdTuples(), 10)
  }

  consumeFields (pq) {
    const nfields = pq.nfields()
    for (var x = 0; x < nfields; x++) {
      this.fields.push({
        name: pq.fname(x),
        dataTypeID: pq.ftype(x)
      })
    }
  }

  consumeRows (pq) {
    const tupleCount = pq.ntuples()
    for (var i = 0; i < tupleCount; i++) {
      const row = this._arrayMode ? this.consumeRowAsArray(pq, i) : this.consumeRowAsObject(pq, i)
      this.rows.push(row)
    }
  }

  consumeRowAsObject (pq, rowIndex) {
    const row = { }
    for (var j = 0; j < this.fields.length; j++) {
      const value = this.readValue(pq, rowIndex, j)
      row[this.fields[j].name] = value
    }
    return row
  }

  consumeRowAsArray (pq, rowIndex) {
    const row = []
    for (var j = 0; j < this.fields.length; j++) {
      const value = this.readValue(pq, rowIndex, j)
      row.push(value)
    }
    return row
  }

  readValue (pq, rowIndex, colIndex) {
    var rawValue = pq.getvalue(rowIndex, colIndex)
    if (rawValue === '') {
      if (pq.getisnull(rowIndex, colIndex)) {
        return null
      }
    }
    const dataTypeId = this.fields[colIndex].dataTypeID
    return this._types.getTypeParser(dataTypeId)(rawValue)
  }
}

function buildResult (pq, types, arrayMode) {
  const result = new Result(types, arrayMode)
  result.consumeCommand(pq)
  result.consumeFields(pq)
  result.consumeRows(pq)

  return result
}

module.exports = buildResult
