export class RequestParams {
  useContext?: boolean = false;
  fillDetails?: boolean = false;
  version?: undefined;
  snapshotSet: string = "";
  useAppView: boolean = false;
  cursor: string | undefined = undefined;
  pageCount: number = 1;
  debugOutput: boolean = false;
  // do not save to the database, but still return or output data
  dryRun: boolean = false;
  // Array of labelers to use for this request
  labelers: string[] = [];
}
