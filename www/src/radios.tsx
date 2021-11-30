import {Setter} from "./utils";
import {FormControlLabel, Radio, RadioGroup, styled} from "@mui/material";
import React from "react";
import {green} from "@mui/material/colors";

const StyledRadio = styled(Radio)`
  color: ${green[400]};
  &.Mui-checked {
    color: ${green[600]};
  }
`;
// const StyledRadio = styled(Radio)``;

export const Radios = function<T extends string>({ choices, choice, setChoice }: {
    choices: { label: string, name: T }[],
    choice: T | null,
    setChoice: Setter<T>,
}) {
    return (
        <RadioGroup value={choice || ""} onChange={e => {
            e.persist()
            console.log("onChange:", e.target.name, e.target, e)
            setChoice(e.target.name as any as T)
        }}>{
            choices.map(({label, name}) =>
                <FormControlLabel
                    key={name}
                    name={name}
                    control={<StyledRadio />}
                    checked={name == choice}
                    label={label}
                />
            )
        }</RadioGroup>
    )
}
