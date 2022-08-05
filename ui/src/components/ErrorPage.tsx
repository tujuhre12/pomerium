import Alert from "@mui/material/Alert";
import AlertTitle from "@mui/material/AlertTitle";
import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import List from "@mui/material/List";
import ListItem, { ListItemProps } from "@mui/material/ListItem";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import React, { FC } from "react";
import { CheckCircle, MinusCircle, XCircle } from "react-feather";

import { ErrorPageData, PolicyEvaluationTrace } from "../types";
import SectionFooter from "./SectionFooter";

type PolicyEvaluationTraceDetailsProps = {
  trace: PolicyEvaluationTrace;
} & ListItemProps;
const PolicyEvaluationTraceDetails: FC<PolicyEvaluationTraceDetailsProps> = ({
  trace,
  ...props
}) => {
  return (
    <ListItem {...props}>
      <ListItemIcon>
        {trace.deny ? (
          <XCircle color="red" />
        ) : trace.allow ? (
          <CheckCircle color="green" />
        ) : (
          <MinusCircle color="gray" />
        )}
      </ListItemIcon>
      <ListItemText
        primary={trace.explanation || trace.id}
        secondary={trace.deny || !trace.allow ? trace.remediation : ""}
      />
    </ListItem>
  );
};

export type ErrorPageProps = {
  data: ErrorPageData;
};
export const ErrorPage: FC<ErrorPageProps> = ({ data }) => {
  const traces =
    data?.policyEvaluationTraces?.filter((trace) => !!trace.id) || [];
  console.log("TRACES", traces);
  return (
    <Container maxWidth={false}>
      <Paper sx={{ overflow: "hidden" }}>
        <Stack>
          <Box sx={{ padding: "16px" }}>
            <Alert severity="error">
              <AlertTitle>
                {data?.status || 500}{" "}
                {data?.statusText || "Internal Server Error"}
              </AlertTitle>
              {data?.error || "Internal Server Error"}
            </Alert>
          </Box>
          {traces?.length > 0 ? (
            <List>
              {traces.map((trace) => (
                <PolicyEvaluationTraceDetails trace={trace} key={trace.id} />
              ))}
            </List>
          ) : (
            <></>
          )}
          {data?.requestId ? (
            <SectionFooter>
              <Typography variant="caption">
                If you should have access, contact your administrator with your
                request id {data?.requestId}.
              </Typography>
            </SectionFooter>
          ) : (
            <></>
          )}
        </Stack>
      </Paper>
    </Container>
  );
};
export default ErrorPage;
