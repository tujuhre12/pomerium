import {
  Card,
  CardActionArea,
  CardContent,
  Grid,
  Typography,
} from "@mui/material";
import React, { FC } from "react";

import { Route, RoutesPageData } from "../types";
import SidebarPage from "./SidebarPage";

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
    <SidebarPage>
      <Grid container spacing={2} justifyContent="center">
        {data?.routes?.map((r) => (
          <Grid key={r.id} item sx={{ width: 300 }}>
            <RouteCard route={r} />
          </Grid>
        ))}
      </Grid>
    </SidebarPage>
  );
};
export default RoutesPage;
