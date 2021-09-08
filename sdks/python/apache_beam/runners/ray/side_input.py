import pandas as pd
import ray


class RaySideInput(object):
  def __init__(self, ray_ds: ray.data.Dataset):
    self.ray_ds = ray_ds

  def convert_df(self, df: pd.DataFrame):
    return df


class RayListSideInput(RaySideInput):
  def convert_df(self, df: pd.DataFrame):
    return df.values


class RayDictSideInput(RaySideInput):
  def convert_df(self, df: pd.DataFrame):
    return dict(df.values.tolist())
