FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source
COPY src/ReliableJobs.Api/ReliableJobs.Api.csproj src/ReliableJobs.Api/
RUN dotnet restore src/ReliableJobs.Api/ReliableJobs.Api.csproj
COPY src/ReliableJobs.Api/ src/ReliableJobs.Api/
RUN dotnet publish src/ReliableJobs.Api/ReliableJobs.Api.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/ .
ENV ASPNETCORE_URLS=http://0.0.0.0:8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "ReliableJobs.Api.dll"]
