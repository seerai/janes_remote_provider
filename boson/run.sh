docker run \
    --rm \
    -it \
    -p 8000:8000 \
    -e PORT=8000 \
    -e API_KEY=$API_KEY \
    -e CLIENT_ID=$CLIENT_ID \
    -e CLIENT_SECRET=$CLIENT_SECRET \
    us-central1-docker.pkg.dev/double-catfish-291717/seerai-docker/images/janes-intara:v0.0.${1}
