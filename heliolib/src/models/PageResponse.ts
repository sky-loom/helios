export class PageResponse<T> {
  public data: T[] = [];
  public cursor: string | undefined = undefined;
}
