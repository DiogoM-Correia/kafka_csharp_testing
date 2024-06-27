# Use the official .NET SDK image
FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src

# Copy the csproj and restore as distinct layers
COPY *.csproj ./
RUN dotnet restore

# Copy everything else and build
COPY . ./
RUN dotnet publish -c Release -o /app

# Use the official .NET runtime image
FROM mcr.microsoft.com/dotnet/runtime:6.0
WORKDIR /app
COPY --from=build /app .

# Define the entry point for the container
ENTRYPOINT ["dotnet", "KafkaConsumer.dll"]  # For KafkaConsumer