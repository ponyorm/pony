<?xml version="1.0"?>
<xsl:stylesheet version = '1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>
  
  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

  <xsl:variable name="styles" select="/html/head/link[@blueprint or (@rel='stylesheet' and @type='text/css')] | /html/head/style" />

  <xsl:template match="head">
    <head>
      <xsl:apply-templates select="@*" />
      <xsl:if test="not(boolean($styles))">
        <xsl:call-template name="blueprint-std" />
      </xsl:if>
      <xsl:apply-templates select="*" />
    </head>
  </xsl:template>

  <xsl:template match="body">
    <body>
      <xsl:choose>
        <xsl:when test="$styles or div[contains(concat(' ', @class, ' '), ' container ')]">
          <xsl:apply-templates select="node()" />
        </xsl:when>
        <xsl:otherwise>
          <div class="container">
            <xsl:apply-templates select="node()" />
          </div>
        </xsl:otherwise>
      </xsl:choose>
    </body>
  </xsl:template>

  <xsl:template name="blueprint-std" match="link[@blueprint]">
    <link rel="stylesheet" href="/pony/static/blueprint/screen.css" type="text/css" media="screen, projection" />
    <link rel="stylesheet" href="/pony/static/blueprint/print.css" type="text/css" media="print" />
    <xsl:comment>{{[if IE]}}</xsl:comment>
    <link rel="stylesheet" href="/pony/static/blueprint/ie.css" type="text/css" media="screen, projection" />
    <xsl:comment>{{[endif]}}</xsl:comment>
  </xsl:template>
  
</xsl:stylesheet>
