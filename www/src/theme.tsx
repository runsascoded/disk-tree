import { createTheme } from '@mui/material/styles';
import { orange, red } from '@mui/material/colors';

const theme = createTheme({
    status: {
        danger: red[500],
    },
    palette: {
        primary: {
            main: orange[500],
        },
    },
});

export default theme;
