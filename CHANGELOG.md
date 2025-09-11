# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Updated Python requirement from 3.9 to 3.10+ to resolve fastmcp dependency issue for Glama.ai deployment
- Added glama.json configuration file for Glama.ai server verification

## [1.1.0] - 2025-07-14

### Added
- **DXT Extension Support**: Complete restructuring to support Anthropic DXT (Desktop Extensions) packaging format
- DXT-compliant directory structure with `server/` containing bundled dependencies and entry point
- New `server/main.py` entry point following official DXT conventions
- Automated DXT package building via GitHub Actions workflow (`.github/workflows/dxt-release.yml`)
- Bundled dependencies in `server/lib/` directory for self-contained extension deployment
- Updated `manifest.json` with proper DXT configuration and PYTHONPATH setup
- DXT installation instructions as primary recommended installation method in README
- Automated release workflow that builds both Docker images and DXT packages
- DXT package validation and testing scripts

### Changed
- **Project Structure**: Restructured from `src/` layout to DXT-compliant `server/` layout
- **Installation Priority**: DXT extension is now the primary recommended installation method
- **README.md**: Completely reorganized to prioritize DXT installation while preserving all existing content
- **Dependency Management**: Moved from runtime dependency installation to bundled dependencies approach
- **Entry Point**: Changed from `python -m propublica_mcp.server` to `python server/main.py`
- **PYTHONPATH Configuration**: Updated to use `${__dirname}/server/lib:${__dirname}/server` for DXT compatibility

### Fixed
- Module resolution issues when running as DXT extension by implementing proper sys.path setup
- Virtual environment compatibility by bundling all dependencies in extension package
- Runtime dependency conflicts by using vendored dependencies approach

### Technical Details
- Maintains full backward compatibility with existing Docker and Python installation methods
- Supports hybrid deployment strategy (DXT for local, Docker for production)
- Automatic GitHub release creation with both Docker images and DXT packages
- Enhanced CI/CD pipeline for multi-format package distribution

## [1.0.0] - 2025-06-17

### Changed
- **BREAKING**: Implemented proper Streamable HTTP transport according to MCP 2025-03-26 specification
- **BREAKING**: Server now uses single `/` endpoint for all MCP communication instead of separate `/sse` and `/messages`
- **BREAKING**: HTTP transport now properly handles both GET and POST requests as per MCP specification
- Tool responses now use correct MCP format with `content` array and `isError` flag

### Fixed
- FastMCP integration for proper tool listing and calling
- Improved JSON-RPC message handling and error responses
- Enhanced session management and security validation
- Updated health check endpoint with transport and version information

### Added
- N/A

### Changed
- N/A

### Fixed
- N/A

## [0.2.0] - 2025-06-16

### Added
- HTTP/SSE server support for cloud deployment compatibility
- Auto-detection of cloud deployment environments (PORT, DO_APP_URL, etc.)
- Health check endpoints (`/health` and `/`) for cloud platform monitoring
- Starlette and Uvicorn dependencies for HTTP server functionality
- Support for both stdio (local MCP usage) and HTTP/SSE (cloud deployment) modes
- DigitalOcean App Platform deployment configuration with PORT environment variable

### Changed
- Server automatically switches between stdio and HTTP modes based on environment
- Enhanced deployment template for DigitalOcean with proper cloud configuration
- Moved integration tests to proper `/tests/` directory for better organization
- Updated import paths in test files for correct module resolution

### Fixed
- DigitalOcean deployment template source conflict (git vs github configuration)
- Docker LABEL parsing error caused by unescaped apostrophe in description
- Docker build failures on cloud platforms due to invalid LABEL syntax
- Project structure cleanup by removing accidentally committed virtual environment

## [0.1.6] - 2025-06-16

### Added
- New `get_most_recent_pdf` MCP tool that finds the most recent Form 990 PDF filing for an organization
- New `get_most_recent_pdf_filing` method in API client that iterates through all filings to find the newest PDF
- One-click deployment buttons for DigitalOcean App Platform and Cloudflare Workers
- Comprehensive deployment guide (`DEPLOYMENT.md`) for cloud platforms
- Cloud deployment configurations (`.do/deploy.template.yaml` and `wrangler.toml`)

### Changed
- Improved PDF detection logic to iterate through all filings starting from most recent year
- Updated README with deployment buttons and cloud deployment instructions

### Fixed
- PDF availability detection now correctly finds older PDFs when recent years don't have PDFs available
- Fixed issue where organizations with PDFs available would incorrectly show "no PDFs found"

## [0.1.5] - 2025-06-16

### Fixed
- Docker tag mismatch in GitHub workflow where images were published without `v` prefix (e.g., `0.1.3`) but workflow was trying to reference them with `v` prefix (e.g., `v0.1.3`)
- Updated all Docker tag references in workflow to strip `v` prefix for consistency with published image tags

## [0.1.4] - 2025-06-16

### Fixed
- Docker test timing issues by implementing retry logic and longer wait periods
- Package description not appearing in GitHub Container Registry
- Removed conflicting metadata labels from workflow that overrode Dockerfile labels
- Enhanced Docker image description with comprehensive details about functionality

### Changed
- Improved Docker test reliability with proper tag resolution for releases
- Increased propagation wait time to 30 seconds with retry mechanism

## [0.1.3] - 2025-06-16

### Fixed
- Docker "manifest unknown" error by correcting GitHub repository URL in Dockerfile
- Repository linking issues by adding `org.opencontainers.image.source` label to workflow metadata
- Image availability timing issues in GitHub Actions workflow
- GITHUB_TOKEN permissions for container package publishing
- Compliance with GitHub Container Registry best practices per official documentation

## [0.1.2] - 2025-06-16

### Added
- New `search_nonprofits_with_pdfs` MCP tool for finding organizations with PDF filings
- `_has_valid_pdf()` helper method to validate PDF URL availability
- `get_organizations_with_pdfs()` method for PDF-specific organization searches
- Enhanced filtering to ensure both `have_pdfs=true` AND valid `pdf_url` fields

### Fixed
- Docker workflow "manifest unknown" error by correcting tag references in GitHub Actions
- Trivy security scan image reference issues
- Docker image testing and publishing pipeline reliability

## [0.1.1] - 2025-06-16

### Fixed
- Corrected release date in CHANGELOG.md for v0.1.0

## [0.1.0] - 2025-06-16

### Added
- Initial project structure setup
- ProPublica MCP Server implementation plan
- Entity Relationship Diagram for data modeling
- Project documentation (README, requirements, etc.)
- Basic FastMCP server framework integration
- Complete MCP server implementation with 6 tools:
  - `search_nonprofits` - Search for nonprofit organizations
  - `get_organization` - Get detailed organization information
  - `get_organization_filings` - Get Form 990 filings
  - `analyze_nonprofit_financials` - Analyze financial trends
  - `search_similar_nonprofits` - Find similar organizations
  - `export_nonprofit_data` - Export data for CRM integration
- Comprehensive data models with validation
- API client with rate limiting and error handling
- Full test suite with 30+ tests passing
- Docker support with multi-stage builds
- Docker Compose configuration for easy deployment
- GitHub Actions workflow for automated Docker publishing
- GitHub Container Registry integration
- Multi-architecture Docker images (linux/amd64, linux/arm64)
- Security scanning with Trivy
- Docker publishing script for manual deployments
- Proper logging configuration for MCP protocol compatibility

### Changed
- N/A

### Deprecated
- N/A

### Removed
- N/A

### Fixed
- N/A

### Security
- N/A 