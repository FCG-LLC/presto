cd $WORKSPACE/source

if test "${branch#*tags/}" != "$branch"; then
	VERSION="target\/apache-presto-${branch#tags/}"
    VERSION_CONTROL="Version: ${branch#tags/}"
else
	SHORT_COMMIT=`expr substr $GIT_COMMIT 1 7`
	VERSION="target\/apache-presto-\${project.version\}-\${maven.build.timestamp\}-$SHORT_COMMIT-$destEnv"
    VERSION_CONTROL="Version: [[project.version]]-[[buildTimestamp]]-$SHORT_COMMIT-$destEnv"   
fi

sed -i "s/Version.*/$VERSION_CONTROL/" presto-server/src/deb/control/control
sed -i "s/<deb.*deb>/<deb>$VERSION.deb<\/deb>/" presto-server/pom.xml
mvn -e -DskipTests -P deb install
cd target
PRESTO_DEB=`ls | grep presto | grep deb`
APTLY_SERVER=http://10.12.1.225:8080
curl -X POST -F file=@$PRESTO_DEB http://10.12.1.225:8080/api/files/$PRESTO_DEB
curl -X POST http://10.12.1.225:8080/api/repos/main/file/$PRESTO_DEB
ssh -tt -i ~/.ssh/aptly_rsa yapee@10.12.1.225

echo version="$VERSION" > env.properties

cd $WORKSPACE/source
cd dockerization
docker build --build-arg destEnv=$destEnv --no-cache -t cs/$app .
docker tag cs/$app portus.cs.int:5000/$destEnv/cs-$app
docker push portus.cs.int:5000/$destEnv/cs-$app
