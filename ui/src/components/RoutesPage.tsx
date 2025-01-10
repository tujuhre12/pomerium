import {
  Card,
  CardActionArea,
  CardContent,
  Grid,
  Typography,
} from "@mui/material";
import React, { FC } from "react";

import { Route, RoutesPageData } from "../types";

type RouteCardProps = {
  route: Route;
};
const RouteCard: FC<RouteCardProps> = ({ route }) => {
  return (
    <Card variant="outlined">
      <CardActionArea href={route.from} target="_blank">
        <CardContent>
          <Typography>{route.name}</Typography>
        </CardContent>
      </CardActionArea>
    </Card>
  );
};

type RoutesPageProps = {
  data: RoutesPageData;
};
const RoutesPage: FC<RoutesPageProps> = ({ data }) => {
  return (
    <>
      <Grid container spacing={2} justifyContent="center">
        {data?.routes?.map((r) => (
          <Grid item sx={{ width: 300 }}>
            <RouteCard route={r} />
          </Grid>
        ))}
      </Grid>
    </>
  );
};
export default RoutesPage;
