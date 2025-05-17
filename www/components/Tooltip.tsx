import { Tooltip as Tooltip0, tooltipClasses, TooltipProps } from "@mui/material"
import { styled } from "@mui/material/styles"

export const Tooltip = styled(({ className, ...props }: TooltipProps) => (
  <Tooltip0 {...props} classes={{ popper: className }} />
))(({ theme }) => ({
  [`& .${tooltipClasses.tooltip}`]: {
    backgroundColor: theme.palette.common.white,
    color: 'rgba(0, 0, 0, 0.87)',
    boxShadow: theme.shadows[1],
    fontSize: '1rem',
  },
}));
