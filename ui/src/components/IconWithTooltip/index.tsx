import React, { ReactNode } from "react"
import { PopperHover, Tooltip, Placement } from "components"

type Props = {
  icon: ReactNode
  tooltip: string
  placement: Placement
}

export const IconWithTooltip = ({ icon, tooltip, placement }: Props) => {
  return (
    <PopperHover
      modifiers={[
        {
          name: "offset",
          options: {
            offset: [-15, 0],
          },
        },
      ]}
      placement={placement}
      trigger={icon}
    >
      <Tooltip>{tooltip}</Tooltip>
    </PopperHover>
  )
}
