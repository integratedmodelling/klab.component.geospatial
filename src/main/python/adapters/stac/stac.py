import argparse
import requests
import sys
import rasterio
from rasterio.io import MemoryFile
from rasterio.merge import merge


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="STAC Adapter")
    parser.add_argument(
        "--bbox",
        nargs=4,          # exactly 4 arguments
        type=float,       # convert each to float
        required=True,
        help="Bounding box: xmin ymin xmax ymax"
    )

    parser.add_argument(
        "--time",
        nargs=2,
        type=str,
        required=True,
        help="Start and end date in ISO format"
    )

    parser.add_argument("--asset", type=str, required=True, help="Asset Id of the STAC")
    parser.add_argument("--band", type=int, required=True, help="Band number to read from the asset")
    parser.add_argument("--collection", type=str, required=True, help="Collection URL of the STAC")
    parser.add_argument("--output", type=str, required=True, help="Output file path")
    args = parser.parse_args()
    catalog_url = None

    resp = requests.get(args.collection)
    if resp.status_code == 200:
       links = resp.json().get("links", [])
       for link in links:
           if link.get("rel") in ["root", "parent"]:
               catalog_url = link.get("href")
               break
    else:
       print(f"Failed to retrieve collection: {resp.status_code}")
       sys.exit(1)

    if not catalog_url:
       print("Catalog URL not found in collection links.")
       sys.exit(1)

    
    ## Search API
    search_endpoint = f"{catalog_url}/search"
    search_payload = {
        "limit": 100,
        "collections": [args.collection.split("/")[-1]],
        "bbox": args.bbox,
        "filter-lang": "cql2-json",
    }

    try:
        r = requests.post(search_endpoint, json=search_payload, timeout=(3, 5))
    except requests.exceptions.Timeout:
        print("The request timed out")
        sys.exit(2) ## Timeout
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        sys.exit(2) ## Request Exception
    
    if r.status_code != 200:
        print(f"Search request failed with status code: {r.status_code}")
        sys.exit(3) ## Search Failed

    search_results = r.json()
    if len(search_results.get("features", [])) == 0:
        print("No features found for the given parameters.")
        sys.exit(4) ## No Features found
    
    features = search_results["features"]
    srcs = []
    for feature in features:
        assets = feature.get("assets", {})
        if args.asset in assets:
            asset_info = assets[args.asset]
            asset_href = asset_info.get("href")
            if asset_href:
                try:
                    with rasterio.open(asset_href) as src:
                        window = rasterio.windows.from_bounds(
                        args.bbox[0], args.bbox[1], args.bbox[2], args.bbox[3],
                        transform=src.transform)
                        window = window.round_offsets().round_lengths()
                        data = src.read(args.band + 1, window=window)
                        profile = src.profile.copy()
                        profile.update(count=1)  # Only one band
                        memfile = MemoryFile()
                        with memfile.open(**profile) as mem:
                            mem.write(data, 1)
                        srcs.append(memfile.open())    
                except requests.exceptions.Timeout:
                    print("The download request timed out")
                    sys.exit(6) ## Download Timeout
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred during download: {e}")
                    sys.exit(6) ## Download Exception
    
    mosaic, out_transform = merge(srcs)
    mosaic = mosaic.squeeze()
    profile.update({
        'driver': 'GTiff',
        'height': mosaic.shape[0],
        'width': mosaic.shape[1],
        'transform': out_transform,
        'count': 1
    })

    # Write to disk
    with rasterio.open(args.output, "w", **profile) as dst:
        dst.write(mosaic, 1)

    exit(0) ## Success!




