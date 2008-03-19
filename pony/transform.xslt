<?xml version="1.0"?>
<xsl:stylesheet version = '1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'
                xmlns:python='python' exclude-result-prefixes='python'>

  <xsl:variable name="conversation">
    <xsl:value-of select="python:conversation()" />
  </xsl:variable>
  <xsl:variable name="styles" select="/html/head/link[@default or (@rel='stylesheet' and @type='text/css')] | /html/head/style" />

  <xsl:template match="/ | @* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

  <xsl:template match="head">
    <head>
      <xsl:apply-templates select="@*" />
      <xsl:if test="base">
        <xsl:variable name="base-url" select="python:set-base-url(base[1]/@href)" />
        <xsl:copy-of select="base[1]" />
      </xsl:if>
      <xsl:if test="not(boolean($styles))">
        <xsl:call-template name="default" />
      </xsl:if>
      <xsl:apply-templates select="*" />
    </head>
  </xsl:template>

  <xsl:template match="base" />

  <xsl:template name="jquery" match="link[@jquery]">
    <script src="/pony/static/jquery/jquery-1.2.3.js" language="JavaScript" type="text/javascript"></script>
  </xsl:template>

  <xsl:template name="default" match="link[@default]">
    <xsl:choose>
      <xsl:when test="@blueprint and @blueprint != ''">
        <link rel="stylesheet" href="/pony/blueprint/{@blueprint}/screen.css" type="text/css" media="screen, projection" />
        <link rel="stylesheet" href="/pony/blueprint/{@blueprint}/print.css" type="text/css" media="print" />
        <xsl:comment>{{[if IE]}}</xsl:comment>
        <link rel="stylesheet" href="/pony/blueprint/{@blueprint}/ie.css" type="text/css" media="screen, projection" />
        <xsl:comment>{{[endif]}}</xsl:comment>
      </xsl:when>
      <xsl:otherwise>
        <link rel="stylesheet" href="/pony/static/blueprint/screen.css" type="text/css" media="screen, projection" />
        <link rel="stylesheet" href="/pony/static/blueprint/print.css" type="text/css" media="print" />
        <xsl:comment>{{[if IE]}}</xsl:comment>
        <link rel="stylesheet" href="/pony/static/blueprint/ie.css" type="text/css" media="screen, projection" />
        <xsl:comment>{{[endif]}}</xsl:comment>
      </xsl:otherwise>
    </xsl:choose>
    <link rel="stylesheet" href="/pony/static/css/default.css" type="text/css" media="screen, projection" />
  </xsl:template>

  <xsl:template match="link[@rounded-corners]">
    <link href="/pony/static/css/rounded-corners.css" type="text/css" rel="stylesheet" />
  </xsl:template>
  
  <xsl:template match="body">
    <body>
      <xsl:apply-templates select="@*" />
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

  <xsl:template match="form">
    <xsl:copy>
      <xsl:if test="string($conversation)">
        <input type="hidden" name="_c" value="{string($conversation)}" />
      </xsl:if>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>
  
  <xsl:template match="@href[not(parent::link)]">
    <xsl:attribute name="href">
      <xsl:value-of select="python:url(string(.))"/>
    </xsl:attribute>
  </xsl:template>

  <xsl:template match="@src[parent::frame or parent::iframe]">
    <xsl:attribute name="src">
      <xsl:value-of select="python:url(string(.))"/>
    </xsl:attribute>
  </xsl:template>

  <xsl:attribute-set name="honeypot-attrs">
    <xsl:attribute name="onkeydown">this.onmousedown()</xsl:attribute>
    <xsl:attribute name="onmousedown">var i=0; while(1&lt;2){i++};</xsl:attribute>
  </xsl:attribute-set>

  <xsl:template match="a[starts-with(@href, 'mailto:') and not(@onmousedown) and not(@onkeydown) and not(@no-obfuscated)]">
    <xsl:variable name="at">
      <xsl:choose>
        <xsl:when test="@at"><xsl:value-of select="@at"/></xsl:when>
        <xsl:otherwise>ATSIGN</xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="dot">
      <xsl:choose>
        <xsl:when test="@dot"><xsl:value-of select="@dot"/></xsl:when>
        <xsl:otherwise>DOT</xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:variable name="address" select="substring(@href, 8)" />
    <xsl:variable name="obfuscated-address" select="concat('javascript:', python:replace(python:replace($address, '.', string($dot)), '@', string($at)))" />
    <span style="display:none">&#160;<a href="{$obfuscated-address}" xsl:use-attribute-sets="honeypot-attrs">[don't click on this]</a>&#160;&#160;</span>
    <xsl:copy>
      <xsl:apply-templates select="@*" />
      <xsl:attribute name="href"><xsl:value-of select="$obfuscated-address" /></xsl:attribute>
      <xsl:attribute name="onmousedown">this.href=this.href.replace('javascript:', 'mai'+'lto:').replace(/<xsl:value-of select="$at" />/g,'@').replace(/<xsl:value-of select="$dot" />/g,'.')</xsl:attribute>
      <xsl:attribute name="onkeydown">this.onmousedown()</xsl:attribute>
      <xsl:apply-templates select="node()" />
    </xsl:copy>
  </xsl:template>

  <xsl:template match="@no-obfuscated"></xsl:template>

  <xsl:template match="*[@radius]">
    <xsl:copy>
      <xsl:apply-templates select="@*[name()!='radius']" />
      <xsl:attribute name="class">
        <xsl:value-of select="normalize-space(concat(@class, ' ', 'rounded'))"/>
      </xsl:attribute>
      <xsl:apply-templates select="node()" />
      <div class="top-left radius-{@radius}"></div>
      <div class="top-right radius-{@radius}"></div>
      <div class="bottom-left radius-{@radius}"></div>
      <div class="bottom-right radius-{@radius}"></div>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="img">
    <xsl:copy>
      <xsl:if test="not(@alt)"><xsl:attribute name="alt" /></xsl:if>
      <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>
