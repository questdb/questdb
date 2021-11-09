import React, {
  useContext,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react"
import { QuestContext } from "providers"
import styled from "styled-components"
import { ColumnDefinition, DatasetType, Type } from "utils"
import { VirtualList, ListRange } from "components"
import ResizeObserver from "resize-observer-polyfill"
import { computeColumnWidths, ComputedGridWidths, getQueryLimit } from "./utils"
import Row from "./Row"
import Header from "./Header"
import Scroller from "./Scroller"
import * as QuestDB from "utils"

type Props = {
  columns: ColumnDefinition[] | false
  count: number | false
  initialDataset: DatasetType[] | false
  perPage: number
  type: Type | undefined
  query: string | undefined
}

const Wrapper = styled.div`
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  overflow: auto;
`

const Canvas = styled.div`
  flex: 1;
`

const Grid = ({
  columns,
  count,
  initialDataset,
  perPage,
  type,
  query,
}: Props) => {
  const [dataset, setDataset] = useState<DatasetType[] | false>()
  const [totalCount, setTotalCount] = useState<number | undefined>()
  const [isScrolling, setIsScrolling] = useState(false)
  const [range, setRange] = useState<ListRange>({
    startIndex: 0,
    endIndex: perPage,
  })
  const [queryFrom, setQueryFrom] = useState(0)
  const [queryTo, setQueryTo] = useState(perPage)
  const [
    computedGridWidths,
    setComputedGridWidths,
  ] = useState<ComputedGridWidths | null>()
  const [rowIndexSelected, setRowIndexSelected] = useState<number>(0)
  const { quest } = useContext(QuestContext)
  const wrapperRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    setRowIndexSelected(0)
    setDataset(initialDataset)
  }, [initialDataset])

  useEffect(() => {
    if (count && type === Type.DQL) {
      setTotalCount(count)
    }
  }, [count, type])

  useEffect(() => {
    if (!isScrolling && range) {
      const { from, to } = getQueryLimit(
        range.startIndex,
        range.endIndex,
        perPage,
      )
      if (from !== queryFrom || to !== queryTo) {
        setQueryFrom(from)
        setQueryTo(to)
        void quest.queryRaw(`${query} LIMIT ${from},${to}`).then((result) => {
          if (result.type === QuestDB.Type.DQL) {
            if (result.dataset) {
              setDataset(result.dataset)
            }
          }
        })
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isScrolling])

  useLayoutEffect(() => {
    if (dataset && columns && wrapperRef.current) {
      setComputedGridWidths(
        computeColumnWidths(columns, dataset, wrapperRef.current?.offsetWidth),
      )

      const ro = new ResizeObserver(() => {
        if (wrapperRef.current) {
          setComputedGridWidths(
            computeColumnWidths(
              columns,
              dataset,
              wrapperRef.current?.offsetWidth,
            ),
          )
        }
      })

      if (wrapperRef.current) {
        ro.observe(wrapperRef.current)
      }

      return () => {
        ro.disconnect()
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dataset, columns, wrapperRef])

  const handleScrollingStateChange = (isScrolling: boolean) => {
    setIsScrolling(isScrolling)
  }

  const handleRangeChanged = (range: ListRange) => {
    setRange(range)
  }

  const handleRowClick = (rowIndex: number) => {
    setRowIndexSelected(rowIndex)
  }

  const itemContent = (index: number) => {
    if (!dataset || !columns || !computedGridWidths) return null
    return (
      <Row
        columns={columns}
        computedGridWidths={computedGridWidths}
        data={dataset[index > perPage ? index - queryFrom : index]}
        index={index}
        isScrolling={isScrolling}
        onRowClick={handleRowClick}
        selected={index === rowIndexSelected}
      />
    )
  }

  return (
    <Wrapper ref={wrapperRef}>
      {columns && computedGridWidths && (
        <Header columns={columns} computedGridWidths={computedGridWidths} />
      )}
      {computedGridWidths && (
        <Canvas style={{ width: computedGridWidths.totalWidth }}>
          {count && (
            <VirtualList
              components={{
                Scroller,
              }}
              isScrolling={handleScrollingStateChange}
              itemContent={itemContent}
              overscan={10}
              rangeChanged={handleRangeChanged}
              totalCount={totalCount}
            />
          )}
        </Canvas>
      )}
    </Wrapper>
  )
}

export default Grid
